"""sources.py — declarative external-source sync ("connectors").

graphify indexes *files on disk*: the pipeline is
``detect() → extract() → build() → …`` over a directory. Bringing an external
system (SharePoint, OneDrive, a Google Drive folder, a list of web docs) into
the graph therefore means one thing: **get its content onto disk as files**,
then let the unchanged pipeline consume it.

Until now that was ad-hoc — ``graphify add <url>`` for a single URL, or the user
manually ``rclone``-ing a folder. This module promotes it to a **declared,
incremental Source layer**. Sources live in ``graphify.toml``::

    [[source]]
    name = "design-docs"
    type = "sharepoint"          # a mounted library (OneDrive/rclone) → local mirror
    path = "/mnt/sharepoint/Design Docs"
    dest = "corpus/design-docs"
    exclude = ["*.tmp", "~$*"]

    [[source]]
    name = "papers"
    type = "urls"
    urls = ["https://arxiv.org/abs/1706.03762"]
    dest = "corpus/papers"

``graphify sync`` then pulls every source into its ``dest`` and the normal
``/graphify .`` / ``--update`` rebuild picks the files up. Three properties make
this more than a copy loop:

* **Incremental** — a per-source cursor (mtime+size for mirrors, fetched-URL set
  for url lists) is persisted in ``graphify-out/sources.json`` so a re-sync only
  transfers what changed.
* **Tombstones** — a mirror deletes ``dest`` files whose source original
  disappeared, so removed remote docs stop leaving ghost nodes in the graph.
* **Provenance** — each synced file records its origin ``uri`` in the state file,
  the seam a later ``source_uri`` node attribute (click-through to the original
  SharePoint/Confluence page) builds on.

Adding a connector mirrors "Adding a new language extractor" in ARCHITECTURE.md:
implement :class:`Source` (``name``, ``type``, ``sync``) and register it in
:data:`_SOURCE_TYPES`.
"""
from __future__ import annotations

import fnmatch
import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable

from graphify.paths import GRAPHIFY_OUT

__all__ = [
    "SyncResult",
    "Source",
    "LocalMirrorSource",
    "UrlListSource",
    "load_sources",
    "sync_all",
    "state_path",
]

_STATE_FILENAME = "sources.json"
# Mirrors are sized for docs, not disk images; skip anything larger to avoid a
# runaway copy of a stray VM image sitting in a synced folder.
_MAX_MIRROR_BYTES = 100 * 1024 * 1024  # 100 MB


def _load_toml(path: Path) -> dict[str, Any]:
    """Parse a TOML file, using stdlib ``tomllib`` (3.11+) or the ``tomli`` backport."""
    try:
        import tomllib as _toml  # type: ignore[import-not-found]
    except ModuleNotFoundError:  # Python < 3.11 — tomli is a declared dependency
        import tomli as _toml  # type: ignore[import-not-found]
    with open(path, "rb") as fh:
        return _toml.load(fh)


@dataclass
class SyncResult:
    """Outcome of syncing one source. Paths are ``dest``-relative for display."""

    name: str
    type: str
    added: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    unchanged: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.added or self.updated or self.removed)

    def summary(self) -> str:
        parts = [
            f"+{len(self.added)}",
            f"~{len(self.updated)}",
            f"-{len(self.removed)}",
            f"={self.unchanged}",
        ]
        line = f"  {self.name} ({self.type}): {' '.join(parts)}"
        if self.errors:
            line += f"  [{len(self.errors)} error(s)]"
        return line


@runtime_checkable
class Source(Protocol):
    """A declared external source that materializes files into a ``dest`` dir.

    ``sync`` receives the source's slice of persisted state (``{}`` on first run)
    and returns ``(SyncResult, new_state)``. Implementations MUST be pure with
    respect to state — never mutate the dict they are handed — so a ``dry_run``
    caller can discard the result safely.
    """

    name: str
    type: str
    dest: str

    def sync(self, out_root: Path, state: dict, *, dry_run: bool = False) -> tuple[SyncResult, dict]:
        ...


def _resolve_dest(out_root: Path, dest: str) -> Path:
    """Resolve a config-supplied ``dest`` under ``out_root``, refusing escapes.

    ``dest`` is attacker-adjacent (it comes from a committed config file), so an
    absolute path or a ``..`` that climbs out of the project root is rejected
    rather than allowed to scribble anywhere on disk.
    """
    root = out_root.resolve()
    target = (root / dest).resolve()
    if target != root and root not in target.parents:
        raise ValueError(f"source dest {dest!r} escapes project root")
    return target


def _iter_files(base: Path):
    """Yield ``(abs_path, rel_posix)`` for every regular file under ``base``."""
    for path in sorted(base.rglob("*")):
        if path.is_file() and not path.is_symlink():
            yield path, path.relative_to(base).as_posix()


@dataclass
class LocalMirrorSource:
    """Mirror a local directory (a mounted remote library) into ``dest``.

    This is the workhorse connector: SharePoint, OneDrive and Google Drive all
    surface as a synced local folder (via the vendor client or ``rclone mount``),
    so ``type = "sharepoint" | "onedrive" | "gdrive" | "local"`` all route here.
    Change detection is ``(mtime_ns, size)`` per file — no hashing, so a big
    library re-syncs cheaply — and files that vanish from the source are removed
    from ``dest`` (tombstones) so their graph nodes don't linger.
    """

    name: str
    type: str
    dest: str
    path: str
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    uri_base: str = ""

    def _selected(self, rel: str) -> bool:
        if self.include and not any(fnmatch.fnmatch(rel, pat) for pat in self.include):
            return False
        if any(fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(Path(rel).name, pat) for pat in self.exclude):
            return False
        return True

    def _uri(self, rel: str) -> str:
        if not self.uri_base:
            return ""
        return self.uri_base.rstrip("/") + "/" + rel

    def sync(self, out_root: Path, state: dict, *, dry_run: bool = False) -> tuple[SyncResult, dict]:
        result = SyncResult(name=self.name, type=self.type)
        src = Path(os.path.expanduser(self.path))
        if not src.is_dir():
            result.errors.append(f"source path not found: {self.path}")
            return result, state

        dest = _resolve_dest(out_root, self.dest)
        prev_files: dict[str, dict] = dict(state.get("files", {}))
        new_files: dict[str, dict] = {}
        seen: set[str] = set()

        for abs_path, rel in _iter_files(src):
            if not self._selected(rel):
                continue
            try:
                st = abs_path.stat()
            except OSError as exc:
                result.errors.append(f"stat {rel}: {exc}")
                continue
            if st.st_size > _MAX_MIRROR_BYTES:
                result.errors.append(f"skip {rel}: exceeds {_MAX_MIRROR_BYTES} bytes")
                continue
            seen.add(rel)
            meta = {"mtime_ns": st.st_mtime_ns, "size": st.st_size, "uri": self._uri(rel)}
            prev = prev_files.get(rel)
            is_new = prev is None
            is_changed = (not is_new) and (
                prev.get("mtime_ns") != st.st_mtime_ns or prev.get("size") != st.st_size
            )
            if is_new or is_changed:
                if not dry_run:
                    out_file = dest / rel
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(abs_path, out_file)
                (result.added if is_new else result.updated).append(rel)
            else:
                result.unchanged += 1
            new_files[rel] = meta

        # Tombstones: anything we tracked before but no longer see at the source.
        for rel in prev_files:
            if rel not in seen:
                result.removed.append(rel)
                if not dry_run:
                    stale = dest / rel
                    try:
                        stale.unlink()
                    except FileNotFoundError:
                        pass
                    except OSError as exc:
                        result.errors.append(f"remove {rel}: {exc}")

        new_state = {"files": new_files}
        return result, (state if dry_run else new_state)


@dataclass
class UrlListSource:
    """Fetch a declared list of URLs into ``dest`` via :func:`graphify.ingest.ingest`.

    Incremental by URL: a URL already recorded in state is skipped unless
    ``refresh`` is set on the sync run. ``ingest_fn`` is injectable so tests need
    no network.
    """

    name: str
    type: str
    dest: str
    urls: list[str] = field(default_factory=list)
    author: str = ""
    contributor: str = ""
    ingest_fn: Callable[..., Path] | None = None

    def sync(self, out_root: Path, state: dict, *, dry_run: bool = False) -> tuple[SyncResult, dict]:
        result = SyncResult(name=self.name, type=self.type)
        dest = _resolve_dest(out_root, self.dest)
        refresh = bool(state.get("_refresh"))
        fetched: dict[str, dict] = dict(state.get("urls", {}))

        ingest_fn = self.ingest_fn
        if ingest_fn is None:
            from graphify.ingest import ingest as ingest_fn  # lazy — keeps import cheap

        for url in self.urls:
            if url in fetched and not refresh:
                result.unchanged += 1
                continue
            if dry_run:
                (result.updated if url in fetched else result.added).append(url)
                continue
            try:
                saved = ingest_fn(
                    url,
                    dest,
                    author=self.author or None,
                    contributor=self.contributor or None,
                )
            except Exception as exc:  # ingest raises RuntimeError/ValueError on fetch failure
                result.errors.append(f"{url}: {exc}")
                continue
            (result.updated if url in fetched else result.added).append(url)
            fetched[url] = {"file": Path(saved).name, "uri": url}

        new_state = {"urls": fetched}
        return result, (state if dry_run else new_state)


# type string (as written in graphify.toml) -> Source class
_SOURCE_TYPES: dict[str, type] = {
    "local": LocalMirrorSource,
    "sharepoint": LocalMirrorSource,
    "onedrive": LocalMirrorSource,
    "gdrive": LocalMirrorSource,
    "urls": UrlListSource,
    "url": UrlListSource,
}

# constructor kwargs each Source class accepts beyond the common name/type/dest
_SOURCE_FIELDS: dict[type, set[str]] = {
    LocalMirrorSource: {"path", "include", "exclude", "uri_base"},
    UrlListSource: {"urls", "author", "contributor"},
}


def _build_source(entry: dict) -> Source:
    stype = str(entry.get("type", "")).strip().lower()
    cls = _SOURCE_TYPES.get(stype)
    if cls is None:
        raise ValueError(
            f"unknown source type {entry.get('type')!r} "
            f"(known: {', '.join(sorted(_SOURCE_TYPES))})"
        )
    name = str(entry.get("name") or "").strip()
    if not name:
        raise ValueError(f"source of type {stype!r} is missing a 'name'")
    dest = str(entry.get("dest") or "").strip()
    if not dest:
        raise ValueError(f"source {name!r} is missing a 'dest'")
    kwargs: dict[str, Any] = {"name": name, "type": stype, "dest": dest}
    for key in _SOURCE_FIELDS.get(cls, set()):
        if key in entry:
            kwargs[key] = entry[key]
    return cls(**kwargs)


def load_sources(config_path: Path) -> list[Source]:
    """Parse ``[[source]]`` tables from a ``graphify.toml`` into Source instances.

    Returns ``[]`` if the file is absent. Raises ``ValueError`` with a pointed
    message on a malformed or unknown-type entry so ``graphify sync`` can report
    it cleanly.
    """
    if not config_path.is_file():
        return []
    data = _load_toml(config_path)
    raw = data.get("source", [])
    if isinstance(raw, dict):  # a single [source] table rather than [[source]]
        raw = [raw]
    if not isinstance(raw, list):
        raise ValueError("'source' in graphify.toml must be an array of tables ([[source]])")
    sources = [_build_source(entry) for entry in raw]
    names = [s.name for s in sources]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise ValueError(f"duplicate source name(s): {', '.join(sorted(dupes))}")
    return sources


def state_path(out_dir: Path) -> Path:
    return out_dir / _STATE_FILENAME


def _load_state(out_dir: Path) -> dict:
    path = state_path(out_dir)
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(out_dir: Path, state: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path(out_dir).write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sync_all(
    sources: list[Source],
    project_root: Path,
    *,
    out_dir: Path | None = None,
    only: str | None = None,
    dry_run: bool = False,
    refresh: bool = False,
) -> list[SyncResult]:
    """Sync every source (or just ``only``) and persist the combined cursor state.

    ``project_root`` anchors each source's ``dest``; ``out_dir`` (default
    ``project_root/graphify-out``) holds ``sources.json``. On ``dry_run`` nothing
    is copied and state is left untouched.
    """
    if out_dir is None:
        out_dir = project_root / GRAPHIFY_OUT
    state = _load_state(out_dir)
    per_source: dict[str, dict] = dict(state.get("sources", {}))
    results: list[SyncResult] = []

    for source in sources:
        if only and source.name != only:
            continue
        prior = dict(per_source.get(source.name, {}))
        if refresh:
            prior["_refresh"] = True
        result, new_state = source.sync(project_root, prior, dry_run=dry_run)
        results.append(result)
        if not dry_run:
            per_source[source.name] = new_state

    if not dry_run:
        state["sources"] = per_source
        _save_state(out_dir, state)
    return results

"""Tests for the declarative external-source sync layer (graphify/sources.py).

Pure unit tests: LocalMirrorSource works entirely within tmp_path, and
UrlListSource is driven with an injected fake ingest fn so there is no network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from graphify.sources import (
    LocalMirrorSource,
    UrlListSource,
    load_sources,
    state_path,
    sync_all,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# --------------------------------------------------------------------------- #
# LocalMirrorSource
# --------------------------------------------------------------------------- #

def test_local_mirror_initial_copy(tmp_path):
    src = tmp_path / "remote"
    _write(src / "a.md", "alpha")
    _write(src / "docs" / "b.md", "beta")
    source = LocalMirrorSource(name="lib", type="sharepoint", dest="corpus/lib", path=str(src))

    results = sync_all([source], tmp_path)

    assert len(results) == 1
    r = results[0]
    assert sorted(r.added) == ["a.md", "docs/b.md"]
    assert r.updated == [] and r.removed == [] and r.unchanged == 0
    assert (tmp_path / "corpus/lib/a.md").read_text() == "alpha"
    assert (tmp_path / "corpus/lib/docs/b.md").read_text() == "beta"
    # state persisted under graphify-out/sources.json
    state = json.loads(state_path(tmp_path / "graphify-out").read_text())
    assert set(state["sources"]["lib"]["files"]) == {"a.md", "docs/b.md"}


def test_local_mirror_incremental_only_changed(tmp_path):
    src = tmp_path / "remote"
    _write(src / "a.md", "alpha")
    _write(src / "b.md", "beta")
    source = LocalMirrorSource(name="lib", type="local", dest="corpus/lib", path=str(src))
    sync_all([source], tmp_path)

    # modify only a.md; bump its mtime so the (mtime,size) cursor sees the change
    (src / "a.md").write_text("alpha-2")
    import os
    os.utime(src / "a.md", (10_000, 10_000))

    r = sync_all([source], tmp_path)[0]
    assert r.updated == ["a.md"]
    assert r.added == [] and r.removed == []
    assert r.unchanged == 1  # b.md untouched
    assert (tmp_path / "corpus/lib/a.md").read_text() == "alpha-2"


def test_local_mirror_tombstone_removes_deleted(tmp_path):
    src = tmp_path / "remote"
    _write(src / "a.md", "alpha")
    _write(src / "gone.md", "bye")
    source = LocalMirrorSource(name="lib", type="onedrive", dest="corpus/lib", path=str(src))
    sync_all([source], tmp_path)
    assert (tmp_path / "corpus/lib/gone.md").exists()

    (src / "gone.md").unlink()
    r = sync_all([source], tmp_path)[0]
    assert r.removed == ["gone.md"]
    assert not (tmp_path / "corpus/lib/gone.md").exists()
    assert (tmp_path / "corpus/lib/a.md").exists()


def test_local_mirror_include_exclude(tmp_path):
    src = tmp_path / "remote"
    _write(src / "keep.md", "k")
    _write(src / "skip.tmp", "t")
    _write(src / "~$lock.md", "lock")
    source = LocalMirrorSource(
        name="lib", type="local", dest="corpus/lib", path=str(src),
        include=["*.md"], exclude=["~$*"],
    )
    r = sync_all([source], tmp_path)[0]
    assert r.added == ["keep.md"]
    assert not (tmp_path / "corpus/lib/skip.tmp").exists()
    assert not (tmp_path / "corpus/lib/~$lock.md").exists()


def test_local_mirror_uri_provenance_recorded(tmp_path):
    src = tmp_path / "remote"
    _write(src / "page.md", "x")
    source = LocalMirrorSource(
        name="lib", type="sharepoint", dest="corpus/lib", path=str(src),
        uri_base="https://contoso.sharepoint.com/sites/eng/Design",
    )
    sync_all([source], tmp_path)
    state = json.loads(state_path(tmp_path / "graphify-out").read_text())
    meta = state["sources"]["lib"]["files"]["page.md"]
    assert meta["uri"] == "https://contoso.sharepoint.com/sites/eng/Design/page.md"


def test_local_mirror_dry_run_writes_nothing(tmp_path):
    src = tmp_path / "remote"
    _write(src / "a.md", "alpha")
    source = LocalMirrorSource(name="lib", type="local", dest="corpus/lib", path=str(src))
    r = sync_all([source], tmp_path, dry_run=True)[0]
    assert r.added == ["a.md"]
    assert not (tmp_path / "corpus/lib/a.md").exists()
    assert not state_path(tmp_path / "graphify-out").exists()


def test_local_mirror_missing_path_reports_error(tmp_path):
    source = LocalMirrorSource(name="lib", type="local", dest="corpus/lib", path=str(tmp_path / "nope"))
    r = sync_all([source], tmp_path)[0]
    assert r.errors and "not found" in r.errors[0]
    assert not r.changed


def test_dest_escape_rejected(tmp_path):
    src = tmp_path / "remote"
    _write(src / "a.md", "alpha")
    source = LocalMirrorSource(name="lib", type="local", dest="../escape", path=str(src))
    with pytest.raises(ValueError, match="escapes project root"):
        sync_all([source], tmp_path)


# --------------------------------------------------------------------------- #
# UrlListSource
# --------------------------------------------------------------------------- #

def test_url_list_fetches_once(tmp_path):
    calls: list[str] = []

    def fake_ingest(url, dest, author=None, contributor=None):
        calls.append(url)
        dest = Path(dest)
        dest.mkdir(parents=True, exist_ok=True)
        f = dest / (url.rsplit("/", 1)[-1] + ".md")
        f.write_text("fetched")
        return f

    source = UrlListSource(
        name="papers", type="urls", dest="corpus/papers",
        urls=["https://example.com/a", "https://example.com/b"],
        ingest_fn=fake_ingest,
    )
    r = sync_all([source], tmp_path)[0]
    assert sorted(r.added) == ["https://example.com/a", "https://example.com/b"]
    assert len(calls) == 2

    # second run: already fetched → skipped, not re-ingested
    r2 = sync_all([source], tmp_path)[0]
    assert r2.added == [] and r2.unchanged == 2
    assert len(calls) == 2


def test_url_list_refresh_refetches(tmp_path):
    calls: list[str] = []

    def fake_ingest(url, dest, author=None, contributor=None):
        calls.append(url)
        Path(dest).mkdir(parents=True, exist_ok=True)
        return Path(dest) / "x.md"

    source = UrlListSource(
        name="papers", type="urls", dest="corpus/papers",
        urls=["https://example.com/a"], ingest_fn=fake_ingest,
    )
    sync_all([source], tmp_path)
    assert len(calls) == 1
    sync_all([source], tmp_path, refresh=True)
    assert len(calls) == 2


def test_url_list_ingest_error_captured(tmp_path):
    def boom(url, dest, author=None, contributor=None):
        raise RuntimeError("fetch failed")

    source = UrlListSource(
        name="papers", type="urls", dest="corpus/papers",
        urls=["https://example.com/a"], ingest_fn=boom,
    )
    r = sync_all([source], tmp_path)[0]
    assert r.added == [] and r.errors and "fetch failed" in r.errors[0]


# --------------------------------------------------------------------------- #
# config loading
# --------------------------------------------------------------------------- #

def test_load_sources_from_toml(tmp_path):
    cfg = tmp_path / "graphify.toml"
    cfg.write_text(
        """
[[source]]
name = "design"
type = "sharepoint"
path = "/mnt/sp/Design"
dest = "corpus/design"
exclude = ["~$*"]

[[source]]
name = "papers"
type = "urls"
dest = "corpus/papers"
urls = ["https://arxiv.org/abs/1706.03762"]
""",
        encoding="utf-8",
    )
    sources = load_sources(cfg)
    assert [s.name for s in sources] == ["design", "papers"]
    assert isinstance(sources[0], LocalMirrorSource)
    assert sources[0].exclude == ["~$*"]
    assert isinstance(sources[1], UrlListSource)
    assert sources[1].urls == ["https://arxiv.org/abs/1706.03762"]


def test_load_sources_absent_file(tmp_path):
    assert load_sources(tmp_path / "nope.toml") == []


def test_load_sources_unknown_type(tmp_path):
    cfg = tmp_path / "graphify.toml"
    cfg.write_text('[[source]]\nname="x"\ntype="notion"\ndest="d"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="unknown source type"):
        load_sources(cfg)


def test_load_sources_missing_dest(tmp_path):
    cfg = tmp_path / "graphify.toml"
    cfg.write_text('[[source]]\nname="x"\ntype="local"\npath="/p"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="missing a 'dest'"):
        load_sources(cfg)


def test_load_sources_duplicate_name(tmp_path):
    cfg = tmp_path / "graphify.toml"
    cfg.write_text(
        '[[source]]\nname="x"\ntype="local"\npath="/p"\ndest="d1"\n'
        '[[source]]\nname="x"\ntype="local"\npath="/q"\ndest="d2"\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate source name"):
        load_sources(cfg)


def test_only_filters_sources(tmp_path):
    src_a = tmp_path / "a"
    src_b = tmp_path / "b"
    _write(src_a / "x.md", "x")
    _write(src_b / "y.md", "y")
    sources = [
        LocalMirrorSource(name="a", type="local", dest="corpus/a", path=str(src_a)),
        LocalMirrorSource(name="b", type="local", dest="corpus/b", path=str(src_b)),
    ]
    results = sync_all(sources, tmp_path, only="a")
    assert [r.name for r in results] == ["a"]
    assert (tmp_path / "corpus/a/x.md").exists()
    assert not (tmp_path / "corpus/b").exists()

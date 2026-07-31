"""Path handling: attachment containment, results directories, session names.

The attachment path comes from the backend and ultimately from a ticket, so it
is untrusted input that gets turned into a local filesystem path.
"""

import os

from famulus_worker import worker


class _FakeResponse:
    """Stand-in for urlopen's context manager, so no network is touched."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return b"payload"


def _fake_download(*args, **kwargs):
    return _FakeResponse()


def test_session_name_is_deterministic_and_short():
    task_id = "2cd15124-73e5-4864-8e17-930b89e1bcad"
    name = worker._session_name(task_id)
    assert name == worker._session_name(task_id)
    assert name.startswith("tr-")
    # The frontend derives the same name; keep it to 12 hex chars after "tr-".
    assert name == "tr-2cd1512473e5"
    assert "-" not in name[3:]


def test_session_names_differ_between_tasks():
    a = worker._session_name("11111111-2222-3333-4444-555555555555")
    b = worker._session_name("99999999-2222-3333-4444-555555555555")
    assert a != b


class TestAttachmentContainment:
    """_download_attachment must never write outside the cache directory."""

    def _run(self, tmp_path, monkeypatch, path):
        monkeypatch.setattr(worker, "ATTACHMENTS_CACHE", str(tmp_path / "cache"))
        return worker._download_attachment("http://api", "tok", {"path": path})

    def test_rejects_path_without_the_expected_shape(self, tmp_path, monkeypatch):
        assert self._run(tmp_path, monkeypatch, "/etc/passwd") is None
        assert self._run(tmp_path, monkeypatch, "") is None

    def test_rejects_traversal_in_the_filename(self, tmp_path, monkeypatch):
        # basename() collapses this to "passwd", but the guard should not rely
        # on the download succeeding — nothing may be created outside the cache.
        assert self._run(tmp_path, monkeypatch, "attachments/abc/../../../etc/passwd") is None

    def test_rejects_a_filename_that_is_only_dots(self, tmp_path, monkeypatch):
        assert self._run(tmp_path, monkeypatch, "attachments/abc/..") is None
        assert self._run(tmp_path, monkeypatch, "attachments/abc/.") is None

    def test_traversal_in_the_file_id_cannot_escape(self, tmp_path, monkeypatch):
        """Regression: `..` as the file id used to write outside the cache.

        The old guard compared the target against a directory built from the
        untrusted id, so once that directory had escaped the check passed.
        """
        cache = tmp_path / "cache"
        monkeypatch.setattr(worker, "ATTACHMENTS_CACHE", str(cache))
        monkeypatch.setattr(worker.urllib.request, "urlopen", _fake_download)

        got = worker._download_attachment("http://api", "tok", {"path": "attachments/../../pwned.txt"})
        assert got is None
        written = [os.path.join(r, f) for r, _, fs in os.walk(tmp_path) for f in fs]
        assert written == [], f"wrote outside the cache: {written}"

    def test_a_download_that_is_allowed_lands_inside_the_cache(self, tmp_path, monkeypatch):
        cache = tmp_path / "cache"
        monkeypatch.setattr(worker, "ATTACHMENTS_CACHE", str(cache))
        monkeypatch.setattr(worker.urllib.request, "urlopen", _fake_download)

        got = worker._download_attachment("http://api", "tok", {"path": "attachments/abc/notes.md"})
        assert got is not None
        assert os.path.realpath(got).startswith(os.path.realpath(cache) + os.sep)
        assert open(got).read() == "payload"

    def test_a_traversing_filename_is_flattened_into_the_cache(self, tmp_path, monkeypatch):
        cache = tmp_path / "cache"
        monkeypatch.setattr(worker, "ATTACHMENTS_CACHE", str(cache))
        monkeypatch.setattr(worker.urllib.request, "urlopen", _fake_download)

        got = worker._download_attachment(
            "http://api", "tok", {"path": "attachments/abc/../../../etc/passwd"}
        )
        # basename() reduces it to a plain name; it must stay under the cache.
        assert got == str(cache / "abc" / "passwd")
        assert os.path.realpath(got).startswith(os.path.realpath(cache) + os.sep)

    def test_returns_the_cached_file_without_downloading(self, tmp_path, monkeypatch):
        cache = tmp_path / "cache"
        monkeypatch.setattr(worker, "ATTACHMENTS_CACHE", str(cache))
        target = cache / "abc" / "notes.md"
        target.parent.mkdir(parents=True)
        target.write_text("already here")

        def explode(*a, **k):  # pragma: no cover - must not be reached
            raise AssertionError("should not download a file already cached")

        monkeypatch.setattr(worker.urllib.request, "urlopen", explode)
        got = worker._download_attachment("http://api", "tok", {"path": "attachments/abc/notes.md"})
        assert got == str(target)


class TestResultsDir:
    def test_uses_a_dot_tr_dir_inside_the_working_directory(self, tmp_path):
        d = worker._results_dir(str(tmp_path), "task-1")
        assert d == str(tmp_path / ".tr" / "task-1")
        assert os.path.isdir(d)

    def test_writes_a_gitignore_so_results_never_get_committed(self, tmp_path):
        worker._results_dir(str(tmp_path), "task-1")
        gitignore = tmp_path / ".tr" / ".gitignore"
        assert gitignore.read_text() == "*\n"

    def test_does_not_clobber_an_existing_gitignore(self, tmp_path):
        base = tmp_path / ".tr"
        base.mkdir()
        (base / ".gitignore").write_text("custom\n")
        worker._results_dir(str(tmp_path), "task-1")
        assert (base / ".gitignore").read_text() == "custom\n"

    def test_falls_back_when_there_is_no_working_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr(worker, "RESULTS_FALLBACK", str(tmp_path / "fallback"))
        d = worker._results_dir(None, "task-9")
        assert d == str(tmp_path / "fallback" / "task-9")
        assert os.path.isdir(d)


class TestDispatchMarkers:
    """Markers are what stop a restart re-running an in-flight stage."""

    def test_a_stage_is_not_dispatched_until_marked(self, tmp_path, monkeypatch):
        monkeypatch.setattr(worker, "DISPATCHED_DIR", str(tmp_path / "d"))
        assert worker._is_stage_dispatched("t1", 0) is False
        worker._mark_stage_dispatched("t1", 0)
        assert worker._is_stage_dispatched("t1", 0) is True

    def test_marks_are_per_stage(self, tmp_path, monkeypatch):
        monkeypatch.setattr(worker, "DISPATCHED_DIR", str(tmp_path / "d"))
        worker._mark_stage_dispatched("t1", 0)
        assert worker._is_stage_dispatched("t1", 1) is False

    def test_clearing_one_task_leaves_another_alone(self, tmp_path, monkeypatch):
        monkeypatch.setattr(worker, "DISPATCHED_DIR", str(tmp_path / "d"))
        worker._mark_stage_dispatched("t1", 0)
        worker._mark_stage_dispatched("t1", 1)
        worker._mark_stage_dispatched("t2", 0)
        worker._clear_dispatch_markers("t1")
        assert worker._is_stage_dispatched("t1", 0) is False
        assert worker._is_stage_dispatched("t1", 1) is False
        assert worker._is_stage_dispatched("t2", 0) is True

    def test_clearing_a_task_with_no_markers_is_harmless(self, tmp_path, monkeypatch):
        monkeypatch.setattr(worker, "DISPATCHED_DIR", str(tmp_path / "missing"))
        worker._clear_dispatch_markers("nope")

    def test_a_task_id_prefix_is_not_treated_as_the_same_task(self, tmp_path, monkeypatch):
        monkeypatch.setattr(worker, "DISPATCHED_DIR", str(tmp_path / "d"))
        worker._mark_stage_dispatched("abc", 0)
        worker._mark_stage_dispatched("abcdef", 0)
        worker._clear_dispatch_markers("abc")
        assert worker._is_stage_dispatched("abcdef", 0) is True

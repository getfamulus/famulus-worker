"""Result collection and the blocked-session detector.

A signed-out `claude` accepts the prompt and then does nothing, so without
detection the step would sit until STEP_TOTAL_TIMEOUT (an hour) with no clue
why. These tests cover each way a dispatched step can be resolved.
"""

import json
import time

import pytest

from famulus_worker import worker


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    """Give each test its own dispatch/session state and no real network."""
    monkeypatch.setattr(worker, "_sessions", {})
    monkeypatch.setattr(worker, "_dispatched", {})
    monkeypatch.setattr(worker, "_run_tokens", {})
    posted: list[tuple[str, dict]] = []
    events: list[dict] = []

    async def fake_post(url, token, payload):
        posted.append((url, payload))
        return True

    async def fake_broadcast(event):
        events.append(event)

    monkeypatch.setattr(worker, "_api_post_async", fake_post)
    monkeypatch.setattr(worker, "broadcast_status", fake_broadcast)
    return {"posted": posted, "events": events}


def dispatch(tmp_path, step_id="s1", task_id="t1", age=0.0, run_token="tok-1"):
    worker._sessions[task_id] = {
        "name": "tr-abc",
        "results_dir": str(tmp_path),
        "logfile": "/dev/null",
        "stages_sent": set(),
        "last_active": time.monotonic(),
    }
    worker._dispatched[step_id] = (task_id, time.monotonic() - age)
    worker._run_tokens[step_id] = run_token


def alive(monkeypatch, is_alive=True, blocked=None):
    async def _alive(name, cache):
        return is_alive

    async def _blocked(name, cache):
        return blocked

    monkeypatch.setattr(worker, "_session_alive", _alive)
    monkeypatch.setattr(worker, "_session_blocked", _blocked)


class TestStrip:
    def test_removes_ansi_colour_codes(self):
        assert worker._strip("\x1b[31mred\x1b[0m") == "red"

    def test_removes_carriage_returns(self):
        assert worker._strip("a\rb") == "ab"

    def test_leaves_plain_text_alone(self):
        assert worker._strip("Login expired") == "Login expired"


class TestBlockedDetection:
    @pytest.mark.parametrize("marker", ["Login expired", "Please run /login", "Invalid API key"])
    def test_recognises_each_blocking_marker(self, marker):
        assert worker._BLOCKED_RE.search(marker)

    def test_is_case_insensitive(self):
        assert worker._BLOCKED_RE.search("login EXPIRED")

    def test_ignores_ordinary_output(self):
        assert not worker._BLOCKED_RE.search("Running tests, all green")

    def test_matches_across_pane_wrapping(self):
        """tmux wraps the pane, so the text must be whitespace-packed first.

        Without the `" ".join(split())` in _session_blocked, a marker split
        across a line break would go unnoticed.
        """
        wrapped = "warning:  Login\n   expired  now"
        assert not worker._BLOCKED_RE.search(wrapped)
        assert worker._BLOCKED_RE.search(" ".join(worker._strip(wrapped).split()))


class TestCollectResults:
    async def test_posts_a_pass_when_the_result_file_says_passed(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        (tmp_path / "s1.json").write_text(json.dumps({"status": "passed", "output": "done"}))

        await worker.collect_results("http://api", "tok")

        assert len(isolate["posted"]) == 1
        url, payload = isolate["posted"][0]
        assert url.endswith("/api/tasks/t1/steps/s1/complete")
        assert payload == {"success": True, "output": "done", "run_token": "tok-1"}
        assert isolate["events"][0]["status"] == "passed"

    async def test_posts_a_failure_when_the_result_says_failed(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        (tmp_path / "s1.json").write_text(json.dumps({"status": "failed", "output": "boom"}))

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"][0][1] == {"success": False, "output": "boom", "run_token": "tok-1"}

    async def test_consumes_the_result_file(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        result = tmp_path / "s1.json"
        result.write_text(json.dumps({"status": "passed"}))

        await worker.collect_results("http://api", "tok")
        assert not result.exists()

    async def test_stops_tracking_a_step_once_reported(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        (tmp_path / "s1.json").write_text(json.dumps({"status": "passed"}))

        await worker.collect_results("http://api", "tok")
        assert "s1" not in worker._dispatched
        await worker.collect_results("http://api", "tok")
        assert len(isolate["posted"]) == 1

    async def test_malformed_json_fails_the_step_rather_than_hanging(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        (tmp_path / "s1.json").write_text("{not json")

        await worker.collect_results("http://api", "tok")
        success, output = isolate["posted"][0][1]["success"], isolate["posted"][0][1]["output"]
        assert success is False
        assert "Invalid result file" in output

    async def test_truncates_a_huge_output(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        (tmp_path / "s1.json").write_text(json.dumps({"status": "passed", "output": "x" * 100_000}))

        await worker.collect_results("http://api", "tok")
        assert len(isolate["posted"][0][1]["output"]) == 50_000

    async def test_waits_while_the_session_is_healthy_and_the_file_is_absent(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"] == []
        assert "s1" in worker._dispatched

    async def test_fails_the_step_after_the_total_timeout(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path, age=worker.STEP_TOTAL_TIMEOUT + 1)

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"][0][1]["success"] is False
        assert "Timed out" in isolate["posted"][0][1]["output"]

    async def test_fails_the_step_when_the_session_died(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch, is_alive=False)
        dispatch(tmp_path)

        await worker.collect_results("http://api", "tok")
        assert "ended before writing a result" in isolate["posted"][0][1]["output"]

    async def test_fails_fast_with_the_reason_when_claude_is_signed_out(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch, blocked="claude cannot run: Login expired. Sign in and re-run.")
        dispatch(tmp_path)

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"][0][1]["success"] is False
        assert "Login expired" in isolate["posted"][0][1]["output"]

    async def test_ignores_a_step_whose_session_is_gone(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        worker._dispatched["orphan"] = ("no-such-task", time.monotonic())

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"] == []


class TestRunToken:
    """The token identifies which execution a result belongs to."""

    async def test_echoes_the_token_it_was_dispatched_with(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path, run_token="abc123")
        (tmp_path / "s1.json").write_text(json.dumps({"status": "passed", "output": "ok"}))

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"][0][1]["run_token"] == "abc123"

    async def test_sends_null_when_the_backend_supplied_none(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path, run_token=None)
        (tmp_path / "s1.json").write_text(json.dumps({"status": "passed"}))

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"][0][1]["run_token"] is None

    async def test_stops_tracking_the_token_once_reported(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path)
        (tmp_path / "s1.json").write_text(json.dumps({"status": "passed"}))

        await worker.collect_results("http://api", "tok")
        assert "s1" not in worker._run_tokens

    async def test_a_timeout_still_reports_its_token(self, tmp_path, isolate, monkeypatch):
        alive(monkeypatch)
        dispatch(tmp_path, age=worker.STEP_TOTAL_TIMEOUT + 1, run_token="xyz")

        await worker.collect_results("http://api", "tok")
        assert isolate["posted"][0][1]["run_token"] == "xyz"

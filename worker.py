#!/usr/bin/env python3
"""Taskrunner worker — connects to backend via reverse WebSocket tunnel.

Runs ONE persistent `claude` session per task inside a detached tmux session,
feeds each pipeline stage into it (fanning out to subagents for parallel jobs),
and relays a live terminal to the browser. No inbound ports required — all
connections are outbound to the backend.

Usage:
    python worker.py --token <TOKEN>
    python worker.py --token <TOKEN> --api http://192.168.0.2:8006
"""

import argparse
import asyncio
import fcntl
import json
import logging
import os
import pty
import re
import select
import shlex
import shutil
import signal
import struct
import subprocess
import termios
import time
import urllib.request
from collections.abc import Awaitable, Callable

import websockets
from websockets.asyncio.client import connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("worker")

CLAUDE_PATH = shutil.which("claude") or "claude"
STEP_TOTAL_TIMEOUT = 3600
SESSION_READY_TIMEOUT = 60
SESSION_IDLE_GRACE = 90
CLAUDE_CONFIG = os.path.expanduser("~/.claude.json")
PANES_DIR = os.path.expanduser("~/.taskrunner/panes")
RESULTS_FALLBACK = os.path.expanduser("~/.taskrunner/results")
STARTED_DIR = os.path.expanduser("~/.taskrunner/started")
DISPATCHED_DIR = os.path.expanduser("~/.taskrunner/dispatched")

SYSTEM_PROMPT = (
    "You are operating in fully autonomous mode as part of an automated pipeline. "
    "Do NOT ask questions, seek clarification, or wait for user input. "
    "Make reasonable assumptions and proceed directly with the task. "
    "Execute all necessary actions without hesitation. "
    "If something is ambiguous, pick the most reasonable interpretation and go."
)

_control_ws: websockets.ClientConnection | None = None


async def broadcast_status(event: dict) -> None:
    """Send a status event to the backend relay."""
    global _control_ws
    if not _control_ws:
        return
    try:
        await _control_ws.send(json.dumps({"type": "status_event", "event": event}))
    except Exception:
        pass


# ── API helpers ──────────────────────────────────────────────────────────


def _headers(token: str) -> dict[str, str]:
    h: dict[str, str] = {
        "Content-Type": "application/json",
        "User-Agent": "taskrunner-worker/3.0",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _api_get(url: str, token: str) -> list | dict | None:
    try:
        req = urllib.request.Request(url, headers=_headers(token))
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        log.error("GET %s failed: %s", url, e)
        return None


def _api_post(url: str, token: str, payload: dict) -> bool:
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, headers=_headers(token), method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        log.error("POST %s failed: %s", url, e)
        return False


async def _api_get_async(url: str, token: str) -> list | dict | None:
    """Run the blocking GET off the event loop.

    :param url: Fully qualified endpoint URL
    :param token: Bearer token for the Authorization header
    :return: Decoded JSON body, or None on failure
    """
    return await asyncio.to_thread(_api_get, url, token)


async def _api_post_async(url: str, token: str, payload: dict) -> bool:
    """Run the blocking POST off the event loop.

    :param url: Fully qualified endpoint URL
    :param token: Bearer token for the Authorization header
    :param payload: JSON-serialisable request body
    :return: True on HTTP 200, else False
    """
    return await asyncio.to_thread(_api_post, url, token, payload)


# ── Attachments ──────────────────────────────────────────────────────────


ATTACHMENTS_CACHE = os.path.expanduser("~/.taskrunner/attachments")


def _download_attachment(api_base: str, token: str, att: dict) -> str | None:
    path = att.get("path", "")
    m = re.search(r"attachments/([^/]+)/(.+)$", path)
    if not m:
        return None
    file_id, raw_filename = m.group(1), m.group(2)
    filename = os.path.basename(raw_filename)
    if not filename or filename in {".", ".."}:
        log.error("Rejected attachment with unsafe filename: %r", raw_filename)
        return None
    local_dir = os.path.join(ATTACHMENTS_CACHE, os.path.basename(file_id))
    local_path = os.path.join(local_dir, filename)
    real_dir = os.path.realpath(local_dir)
    if not os.path.realpath(local_path).startswith(real_dir + os.sep):
        log.error("Rejected attachment escaping cache dir: %r", raw_filename)
        return None
    if os.path.isfile(local_path):
        return local_path
    url = f"{api_base}/api/tasks/attachments/{file_id}/{raw_filename}"
    try:
        req = urllib.request.Request(url, headers=_headers(token))
        os.makedirs(local_dir, exist_ok=True)
        with urllib.request.urlopen(req, timeout=30) as resp:
            with open(local_path, "wb") as f:
                f.write(resp.read())
        return local_path
    except Exception as e:
        log.error("Failed to download attachment %s: %s", filename, e)
        return None


async def _download_attachment_async(api_base: str, token: str, att: dict) -> str | None:
    """Run the blocking attachment download off the event loop.

    :param api_base: Backend API base URL
    :param token: Bearer token for the Authorization header
    :param att: Attachment descriptor containing its storage path
    :return: Local cached path, or None on failure
    """
    return await asyncio.to_thread(_download_attachment, api_base, token, att)


# ── tmux + claude session management ─────────────────────────────────────

_ANSI = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z]|\x1b\][^\x07]*(?:\x07|\x1b\\)|\r")


def _strip(s: str) -> str:
    return _ANSI.sub("", s)


def _session_name(task_id: str) -> str:
    """Deterministic tmux session name shared with the frontend (see TaskDetailPage)."""
    return "tr-" + task_id.replace("-", "")[:12]


async def _tmux(*args: str, inp: str | None = None) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        "tmux", *args,
        stdin=asyncio.subprocess.PIPE if inp is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate(inp.encode() if inp is not None else None)
    return proc.returncode, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


def _ensure_trust(path: str) -> None:
    """Pre-accept the workspace trust dialog so interactive claude starts unattended."""
    try:
        if os.path.isfile(CLAUDE_CONFIG):
            with open(CLAUDE_CONFIG) as f:
                data = json.load(f)
        else:
            data = {}
    except (json.JSONDecodeError, OSError):
        return
    projects = data.setdefault("projects", {})
    changed = False
    for p in {path, os.path.realpath(path)}:
        entry = projects.setdefault(p, {})
        if not entry.get("hasTrustDialogAccepted") or not entry.get("hasCompletedProjectOnboarding"):
            entry["hasTrustDialogAccepted"] = True
            entry["hasCompletedProjectOnboarding"] = True
            changed = True
    if changed:
        try:
            with open(CLAUDE_CONFIG, "w") as f:
                json.dump(data, f)
        except OSError as e:
            log.warning("Could not persist trust for %s: %s", path, e)


def _results_dir(working_dir: str | None, task_id: str) -> str:
    if working_dir:
        base = os.path.join(working_dir, ".tr")
        os.makedirs(base, exist_ok=True)
        gitignore = os.path.join(base, ".gitignore")
        if not os.path.isfile(gitignore):
            try:
                with open(gitignore, "w") as f:
                    f.write("*\n")
            except OSError:
                pass
        d = os.path.join(base, task_id)
    else:
        d = os.path.join(RESULTS_FALLBACK, task_id)
    os.makedirs(d, exist_ok=True)
    return d


# task_id -> {name, results_dir, logfile, stages_sent: set[int], last_active}
_sessions: dict[str, dict] = {}
# step_id -> (task_id, monotonic dispatch time) — steps awaiting a result file
_dispatched: dict[str, tuple[str, float]] = {}


def _dispatch_marker(task_id: str, stage_idx: int) -> str:
    return os.path.join(DISPATCHED_DIR, f"{task_id}.{stage_idx}")


def _is_stage_dispatched(task_id: str, stage_idx: int) -> bool:
    """Return whether a stage was already injected in a prior (pre-restart) run.

    :param task_id: Owning task id
    :param stage_idx: Pipeline stage index
    :return: True if a persisted dispatch marker exists
    """
    return os.path.isfile(_dispatch_marker(task_id, stage_idx))


def _mark_stage_dispatched(task_id: str, stage_idx: int) -> None:
    """Persist a marker so a restart does not re-inject an in-flight stage.

    :param task_id: Owning task id
    :param stage_idx: Pipeline stage index
    """
    try:
        os.makedirs(DISPATCHED_DIR, exist_ok=True)
        open(_dispatch_marker(task_id, stage_idx), "w").close()
    except OSError as e:
        log.warning("Could not persist dispatch marker for %s.%d: %s", task_id[:8], stage_idx, e)


def _clear_dispatch_markers(task_id: str) -> None:
    """Remove all persisted dispatch markers for a task.

    :param task_id: Owning task id
    """
    try:
        entries = os.listdir(DISPATCHED_DIR)
    except OSError:
        return
    for entry in entries:
        if entry == task_id or entry.startswith(f"{task_id}."):
            try:
                os.remove(os.path.join(DISPATCHED_DIR, entry))
            except OSError:
                pass


async def _wait_ready(logfile: str) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < SESSION_READY_TIMEOUT:
        try:
            with open(logfile, encoding="utf-8", errors="replace") as f:
                if "auto mode on" in _strip(f.read()):
                    return True
        except OSError:
            pass
        await asyncio.sleep(0.5)
    return False


async def _ensure_session(task_id: str, working_dir: str | None) -> dict | None:
    """Return session state for a task, creating the tmux+claude session if needed."""
    state = _sessions.get(task_id)
    name = _session_name(task_id)
    rc, _, _ = await _tmux("has-session", "-t", name)

    if state and rc == 0:
        return state

    if rc == 0 and not state:
        # Worker restarted mid-task: adopt the live session; it already has context.
        state = {
            "name": name,
            "results_dir": _results_dir(working_dir, task_id),
            "logfile": os.path.join(PANES_DIR, f"{name}.log"),
            "stages_sent": set(),
            "fresh": False,
            "last_active": time.monotonic(),
        }
        _sessions[task_id] = state
        log.info("Adopted existing session %s after restart", name)
        return state

    cwd = working_dir or os.path.expanduser("~")
    _ensure_trust(cwd)
    os.makedirs(PANES_DIR, exist_ok=True)
    os.makedirs(STARTED_DIR, exist_ok=True)
    logfile = os.path.join(PANES_DIR, f"{name}.log")
    open(logfile, "w").close()

    # Claude's conversation id is the task id: create it once, resume it on Continue.
    marker = os.path.join(STARTED_DIR, task_id)
    resuming = os.path.isfile(marker)
    session_flag = f"--resume {shlex.quote(task_id)}" if resuming else f"--session-id {shlex.quote(task_id)}"

    launch = (
        f"cd {shlex.quote(cwd)} && exec {shlex.quote(CLAUDE_PATH)} {session_flag} "
        f"--permission-mode auto --append-system-prompt {shlex.quote(SYSTEM_PROMPT)}"
    )
    rc, _, err = await _tmux("new-session", "-d", "-s", name, "-x", "220", "-y", "50", launch)
    if rc != 0:
        log.error("Failed to start tmux session %s: %s", name, err)
        return None
    await _tmux("pipe-pane", "-o", "-t", name, f"cat >> {shlex.quote(logfile)}")
    if not resuming:
        try:
            open(marker, "w").close()
        except OSError:
            pass

    state = {
        "name": name,
        "results_dir": _results_dir(working_dir, task_id),
        "logfile": logfile,
        "stages_sent": set(),
        "fresh": not resuming,
        "last_active": time.monotonic(),
    }
    _sessions[task_id] = state
    log.info("Started session %s (cwd=%s, %s)", name, cwd, "resumed" if resuming else "new")

    if not await _wait_ready(logfile):
        log.warning("Session %s did not report ready; proceeding anyway", name)
    return state


async def _inject(name: str, text: str) -> None:
    """Paste a prompt into the claude input box and submit it (handles multi-line safely)."""
    await _tmux("load-buffer", "-", inp=text)
    await _tmux("paste-buffer", "-d", "-t", name)
    await asyncio.sleep(0.5)
    await _tmux("send-keys", "-t", name, "Enter")


async def _kill_session(task_id: str) -> None:
    state = _sessions.pop(task_id, None)
    if state:
        await _tmux("kill-session", "-t", state["name"])
        log.info("Killed session %s", state["name"])
    _clear_dispatch_markers(task_id)
    try:
        os.remove(os.path.join(STARTED_DIR, task_id))
    except OSError:
        pass


# ── Prompt building ──────────────────────────────────────────────────────


def _build_stage_prompt(
    steps: list[dict],
    results_dir: str,
    first_stage: bool,
    local_paths: list[str],
) -> str:
    task_title = steps[0]["task_title"]
    task_description = steps[0].get("task_description") or ""
    n = len(steps)

    lines: list[str] = []
    if first_stage:
        lines.append("You are running an automated pipeline for the following task. Work fully autonomously.")
        lines.append("")
        lines.append(f"TASK: {task_title}")
        if task_description:
            lines.append(f"DESCRIPTION: {task_description}")
        if local_paths:
            lines.append("")
            lines.append("Attached files (read with the Read tool):")
            for p in local_paths:
                lines.append(f"  - {p}")
        lines.append("")
        lines.append(
            f"This stage has {n} independent job(s). FIRST gather any shared context needed by the jobs "
            "(e.g. fetch the referenced ticket, read relevant files) EXACTLY ONCE. THEN run the jobs in "
            "parallel by launching one subagent per job with the Task tool, passing each subagent the shared "
            "context you already gathered so it does NOT re-fetch anything."
        )
    else:
        lines.append(
            f"Next stage — {n} job(s). You already have the task context from earlier in this session; do not "
            "re-fetch it. Do any small shared setup once, then run the jobs in parallel via one subagent each "
            "with the Task tool."
        )

    lines.append("")
    lines.append("Jobs:")
    for i, step in enumerate(steps, 1):
        note = step.get("step_note") or ""
        lines.append(f"  {i}. [step_id={step['step_id']}] {step['step_name']}" + (f" — {note}" if note else ""))

    lines.append("")
    lines.append(
        "When each job is finished, record its result with the Write tool to "
        f"`{results_dir}/<step_id>.json` (one file per job, named by that job's step_id) containing JSON: "
        '{"status": "passed" or "failed", "output": "<= 2000 char summary of what was done or why it failed"}. '
        "Write a result file for EVERY job listed above before you finish."
    )
    return "\n".join(lines)


# ── Pipeline driving ─────────────────────────────────────────────────────


async def dispatch_stage(api_base: str, token: str, task_id: str, stage_idx: int, steps: list[dict]) -> None:
    working_dir = steps[0].get("working_dir")
    state = await _ensure_session(task_id, working_dir)
    if not state:
        for step in steps:
            await _api_post_async(
                f"{api_base}/api/tasks/{task_id}/steps/{step['step_id']}/complete",
                token, {"success": False, "output": "Failed to start claude session"},
            )
        return

    if stage_idx in state["stages_sent"]:
        return

    # After a restart the live tmux session is adopted with an empty stages_sent set.
    # A persisted marker means this stage was already injected before the restart, so
    # re-injecting would duplicate the work — adopt its in-flight steps instead.
    if _is_stage_dispatched(task_id, stage_idx):
        state["stages_sent"].add(stage_idx)
        now = time.monotonic()
        for step in steps:
            _dispatched[step["step_id"]] = (task_id, now)
        log.info("Stage %d of task %s already dispatched; adopting in-flight steps", stage_idx, task_id[:8])
        return

    # Full task context is only injected for a brand-new conversation's first stage.
    # A resumed session (Continue) already has all prior context.
    first_stage = state.get("fresh", False) and len(state["stages_sent"]) == 0
    local_paths: list[str] = []
    if first_stage:
        for att in steps[0].get("attachments") or []:
            lp = await _download_attachment_async(api_base, token, att)
            if lp:
                local_paths.append(lp)

    prompt = _build_stage_prompt(steps, state["results_dir"], first_stage, local_paths)
    log.info("Dispatching stage %d of task %s (%d job(s))", stage_idx, task_id[:8], len(steps))
    await _inject(state["name"], prompt)

    state["stages_sent"].add(stage_idx)
    _mark_stage_dispatched(task_id, stage_idx)
    state["last_active"] = time.monotonic()
    now = time.monotonic()
    for step in steps:
        _dispatched[step["step_id"]] = (task_id, now)
        await broadcast_status({
            "type": "step_started",
            "task_id": task_id,
            "step_id": step["step_id"],
            "step_name": step["step_name"],
        })


async def _session_alive(name: str, cache: dict[str, bool]) -> bool:
    """Whether a tmux session is live, memoized within one collect pass.

    Steps of a stage share one session, so this avoids forking an identical
    ``tmux has-session`` per step every poll.

    :param name: tmux session name
    :param cache: Per-pass memo of session name -> liveness
    :return: True if the session exists
    """
    if name not in cache:
        rc, _, _ = await _tmux("has-session", "-t", name)
        cache[name] = rc == 0
    return cache[name]


async def collect_results(api_base: str, token: str) -> None:
    """Post completions for dispatched steps whose result file has appeared (or timed out)."""
    now = time.monotonic()
    alive_cache: dict[str, bool] = {}

    for step_id, (task_id, dispatched_at) in list(_dispatched.items()):
        state = _sessions.get(task_id)
        if not state:
            continue

        result_file = os.path.join(state["results_dir"], f"{step_id}.json")
        success: bool | None = None
        output = ""

        if os.path.isfile(result_file):
            try:
                with open(result_file) as f:
                    data = json.load(f)
                success = data.get("status") == "passed"
                output = str(data.get("output", ""))[:50_000]
            except (json.JSONDecodeError, OSError):
                success, output = False, "Invalid result file written by claude"
            try:
                os.remove(result_file)
            except OSError:
                pass
        elif now - dispatched_at > STEP_TOTAL_TIMEOUT:
            success, output = False, f"Timed out after {STEP_TOTAL_TIMEOUT}s with no result"
        elif not await _session_alive(state["name"], alive_cache):
            success, output = False, "Claude session ended before writing a result"

        if success is None:
            continue

        _dispatched.pop(step_id, None)
        state["last_active"] = now
        await _api_post_async(
            f"{api_base}/api/tasks/{task_id}/steps/{step_id}/complete",
            token, {"success": success, "output": output},
        )
        await broadcast_status({
            "type": "step_completed",
            "task_id": task_id,
            "step_id": step_id,
            "status": "passed" if success else "failed",
        })
        log.info("  step %s: %s", step_id[:8], "passed" if success else "failed")


async def reap_idle_sessions(active_task_ids: set[str]) -> None:
    now = time.monotonic()
    for task_id, state in list(_sessions.items()):
        if task_id in active_task_ids:
            state["last_active"] = now
        elif now - state["last_active"] > SESSION_IDLE_GRACE:
            await _kill_session(task_id)


async def process_reply(api_base: str, token: str, reply: dict) -> None:
    """User replied to a running task — inject the message straight into its live session."""
    task_id = reply["task_id"]
    step_id = reply["step_id"]
    message = reply["message"]
    state = _sessions.get(task_id)
    if not state:
        await _api_post_async(
            f"{api_base}/api/tasks/{task_id}/steps/{step_id}/complete",
            token, {"success": False, "output": "No live session to resume"},
        )
        return
    log.info("Injecting reply into %s: %s", state["name"], message[:60])
    await _inject(state["name"], message)
    _dispatched[step_id] = (task_id, time.monotonic())
    state["last_active"] = time.monotonic()


async def create_worktree(api_base: str, token: str, wt: dict) -> None:
    task_id = wt["task_id"]
    ticket_id = wt["ticket_id"]
    repo_path = wt["repo_path"]
    worktree_script = os.path.expanduser("~/.claude/utils/worktree-new.sh")

    log.info("Creating worktree for %s in %s", ticket_id, repo_path)
    try:
        proc = await asyncio.create_subprocess_exec(
            "bash", worktree_script, ticket_id.lower(),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=repo_path,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        output = stdout.decode("utf-8", errors="replace")

        worktree_path = None
        for line in output.splitlines():
            if line.startswith("WORKTREE_PATH="):
                worktree_path = line.split("=", 1)[1].strip()
                break
        if not worktree_path and wt.get("worktrees_path"):
            worktree_path = os.path.join(wt["worktrees_path"], ticket_id.lower())

        if proc.returncode == 0 and worktree_path:
            log.info("  Worktree created: %s", worktree_path)
            await _api_post_async(f"{api_base}/api/tasks/{task_id}/worktree-done", token, {"worktree_path": worktree_path})
        else:
            log.error("  Worktree failed (rc=%d): %s", proc.returncode, stderr.decode()[:200])
            # Report failure so the task fails instead of being re-polled forever.
            await _api_post_async(f"{api_base}/api/tasks/{task_id}/worktree-failed", token, {})
    except Exception as e:
        log.error("  Worktree error: %s", e)
        await _api_post_async(f"{api_base}/api/tasks/{task_id}/worktree-failed", token, {})


# ── Terminal session handler ─────────────────────────────────────────────


def _blocking_read(fd: int) -> bytes:
    while True:
        r, _, _ = select.select([fd], [], [], 0.5)
        if r:
            return os.read(fd, 4096)
        try:
            os.fstat(fd)
        except OSError:
            return b""


async def handle_terminal_session(ws_base: str, token: str, cmd: dict) -> None:
    """Open a pty and stream it to the backend.

    If a tmux session is requested and live, attach to it (the task's claude session);
    otherwise fall back to a plain login shell in ``cwd``.
    """
    sid = cmd["sid"]
    cwd = cmd.get("cwd") or os.path.expanduser("~")
    cols = cmd.get("cols", 120)
    rows = cmd.get("rows", 30)
    shell = os.environ.get("SHELL", "/bin/zsh")
    tmux_session = cmd.get("tmux_session")
    await_tmux = bool(cmd.get("await_tmux"))

    url = f"{ws_base}/ws/worker/terminal/{sid}"
    log.info("Opening terminal session %s (tmux=%s await=%s)", sid[:8], tmux_session, await_tmux)

    try:
        async with connect(url, max_size=2**20) as ws:
            await ws.send(json.dumps({"type": "auth", "token": token}))
            # Decide what to run. If a task session is requested, attach to it — waiting
            # briefly for the worker to spin it up when the task was just started.
            launch = [shell, "-l"]
            if tmux_session:
                deadline = time.monotonic() + (30 if await_tmux else 0)
                notified = False
                while True:
                    rc, _, _ = await _tmux("has-session", "-t", tmux_session)
                    if rc == 0:
                        launch = ["tmux", "attach", "-t", tmux_session]
                        break
                    if time.monotonic() >= deadline:
                        break
                    if not notified:
                        await ws.send("\r\n\x1b[90m[waiting for claude session…]\x1b[0m\r\n")
                        notified = True
                    await asyncio.sleep(1)

            master_fd, slave_fd = pty.openpty()
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)

            env = os.environ.copy()
            env["TERM"] = "xterm-256color"
            env["COLUMNS"] = str(cols)
            env["LINES"] = str(rows)

            proc = subprocess.Popen(
                launch,
                stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
                cwd=cwd, env=env, preexec_fn=os.setsid, close_fds=True,
            )
            os.close(slave_fd)

            flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
            fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

            loop = asyncio.get_event_loop()
            done = asyncio.Event()

            async def read_pty() -> None:
                while not done.is_set():
                    try:
                        data = await loop.run_in_executor(None, _blocking_read, master_fd)
                        if not data:
                            break
                        await ws.send(data)
                    except (OSError, websockets.exceptions.ConnectionClosed):
                        break
                done.set()

            async def write_pty() -> None:
                try:
                    async for msg in ws:
                        if isinstance(msg, str):
                            try:
                                parsed = json.loads(msg)
                                if parsed.get("type") == "resize":
                                    ws_data = struct.pack("HHHH", parsed.get("rows", 30), parsed.get("cols", 120), 0, 0)
                                    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, ws_data)
                                    os.kill(proc.pid, signal.SIGWINCH)
                                    continue
                            except (json.JSONDecodeError, KeyError):
                                pass
                            os.write(master_fd, msg.encode())
                        else:
                            os.write(master_fd, msg)
                except (OSError, websockets.exceptions.ConnectionClosed):
                    pass
                done.set()

            try:
                await asyncio.gather(read_pty(), write_pty())
            finally:
                # Detach cleanly: for a tmux attach this leaves the session (and claude) running.
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                try:
                    os.close(master_fd)
                except OSError:
                    pass
                log.info("Terminal session %s closed", sid[:8])
    except Exception as e:
        log.error("Terminal session %s error: %s", sid[:8], e)


# ── Control channel (WebSocket client to backend) ───────────────────────


async def control_loop(ws_base: str, token: str) -> None:
    """Persistent outbound WebSocket to the backend relay."""
    url = f"{ws_base}/ws/worker"
    log.info("Connecting to backend: %s", ws_base)

    async for ws in connect(url, ping_interval=30, ping_timeout=10, max_size=2**20):
        global _control_ws
        try:
            await ws.send(json.dumps({"type": "auth", "token": token}))
        except websockets.ConnectionClosed:
            log.warning("Backend closed during auth, reconnecting...")
            continue
        _control_ws = ws
        log.info("Connected to backend relay")
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "terminal_open":
                        asyncio.create_task(handle_terminal_session(ws_base, token, msg))
                    elif msg.get("type") == "stop_task":
                        task_id = msg.get("task_id", "")
                        await _kill_session(task_id)
                        log.info("Stopped task %s", task_id[:8])
                except json.JSONDecodeError:
                    pass
        except websockets.ConnectionClosed:
            pass
        _control_ws = None
        log.warning("Backend connection lost, reconnecting...")


# ── Polling loop ─────────────────────────────────────────────────────────


async def poll_loop(api_base: str, token: str, poll_interval: float) -> None:
    log.info("Polling %s every %.0fs", api_base, poll_interval)

    while True:
        try:
            # Worktrees
            worktrees = await _api_get_async(f"{api_base}/api/tasks/worktrees/pending", token) or []
            for wt in worktrees:
                await create_worktree(api_base, token, wt)

            # Steps: group running steps by (task, stage) and drive one stage prompt each.
            steps = await _api_get_async(f"{api_base}/api/tasks/steps/pending", token) or []
            if steps:
                groups: dict[tuple[str, int], list[dict]] = {}
                for step in steps:
                    groups.setdefault((step["task_id"], step["stage_idx"]), []).append(step)
                for (task_id, stage_idx), group in sorted(groups.items(), key=lambda kv: kv[0][1]):
                    already_sent = stage_idx in _sessions.get(task_id, {}).get("stages_sent", set())
                    if not already_sent:
                        await dispatch_stage(api_base, token, task_id, stage_idx, group)

            # Replies (steps the user answered — injected straight into the live session)
            replies = await _api_get_async(f"{api_base}/api/tasks/steps/replies", token) or []
            for reply in replies:
                if reply["step_id"] not in _dispatched:
                    await process_reply(api_base, token, reply)

            # Collect finished-job result files and post completions.
            await collect_results(api_base, token)

            # Keep a session alive while it has running steps OR any dispatched job in flight.
            active = {s["task_id"] for s in steps} | {tid for tid, _ in _dispatched.values()}
            await reap_idle_sessions(active)

            await broadcast_status({"type": "heartbeat"})
        except Exception:
            log.exception("poll_loop iteration failed; continuing")

        await asyncio.sleep(poll_interval)


# ── Main ─────────────────────────────────────────────────────────────────


async def _supervise(name: str, factory: Callable[[], Awaitable[None]]) -> None:
    """Restart a long-running coroutine whenever it crashes.

    :param name: Human-readable coroutine name for logging
    :param factory: Zero-arg callable returning a fresh awaitable to run
    """
    while True:
        try:
            await factory()
            log.warning("%s exited cleanly; restarting in 3s", name)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s crashed; restarting in 3s", name)
        await asyncio.sleep(3)


async def main(api_base: str, token: str, poll_interval: float) -> None:
    ws_base = api_base.replace("https://", "wss://").replace("http://", "ws://")
    log.info("Worker starting (reverse tunnel mode)")
    log.info("  API:    %s", api_base)
    log.info("  WS:     %s", ws_base)
    log.info("  Claude: %s", CLAUDE_PATH)

    await asyncio.gather(
        _supervise("control_loop", lambda: control_loop(ws_base, token)),
        _supervise("poll_loop", lambda: poll_loop(api_base, token, poll_interval)),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Taskrunner worker (reverse tunnel)")
    parser.add_argument("--api", default="https://taskrunner.dimash.dev", help="Backend API base URL")
    parser.add_argument("--token", default=os.environ.get("TASKRUNNER_TOKEN", ""), help="Auth token")
    parser.add_argument(
        "--worker-token",
        default=os.environ.get("TASKRUNNER_WORKER_TOKEN", ""),
        help="Worker token for WS auth and REST (falls back to --token/TASKRUNNER_TOKEN)",
    )
    parser.add_argument("--poll-interval", type=float, default=3, help="Seconds between polls")
    args = parser.parse_args()

    token = args.worker_token or args.token
    if not token:
        parser.error(
            "a token is required: set --worker-token/TASKRUNNER_WORKER_TOKEN "
            "or --token/TASKRUNNER_TOKEN"
        )

    try:
        asyncio.run(main(args.api, token, args.poll_interval))
    except KeyboardInterrupt:
        log.info("Worker stopped")

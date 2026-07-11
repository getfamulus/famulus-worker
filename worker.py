#!/usr/bin/env python3
"""Taskrunner worker — connects to backend via reverse WebSocket tunnel.

Runs claude CLI for pipeline steps and provides live terminal sessions.
No inbound ports required — all connections are outbound to the backend.

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
import shutil
import signal
import struct
import subprocess
import termios
import time
import urllib.parse
import urllib.request

import websockets
from websockets.asyncio.client import connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("worker")

CLAUDE_PATH = shutil.which("claude") or "claude"
STEP_IDLE_TIMEOUT = 600
STEP_TOTAL_TIMEOUT = 3600
WORKTREE_SCRIPT = os.path.expanduser("~/.claude/utils/worktree-new.sh")
CLAUDE_SESSIONS_DIR = os.path.expanduser("~/.claude/sessions")

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
        "User-Agent": "taskrunner-worker/2.0",
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


# ── Claude CLI ───────────────────────────────────────────────────────────


def _fix_session_index(session_id: str, cwd: str) -> None:
    """Patch the claude session index so the session appears in `claude --resume` from cwd."""
    if not os.path.isdir(CLAUDE_SESSIONS_DIR):
        return
    for fname in os.listdir(CLAUDE_SESSIONS_DIR):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(CLAUDE_SESSIONS_DIR, fname)
        try:
            with open(path) as f:
                data = json.load(f)
            if data.get("sessionId") == session_id and data.get("cwd") != cwd:
                data["cwd"] = cwd
                with open(path, "w") as f:
                    json.dump(data, f)
                log.debug("Fixed session index cwd for %s", session_id)
                return
        except (json.JSONDecodeError, OSError):
            continue


_running_procs: dict[str, asyncio.subprocess.Process] = {}


IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}


def _build_stream_input(prompt: str, image_paths: list[str]) -> str:
    """Build a stream-json input message with text and inline images."""
    import base64
    content: list[dict] = [{"type": "text", "text": prompt}]
    for path in image_paths:
        ext = os.path.splitext(path)[1].lower()
        media_type = {
            ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
        }.get(ext, "image/png")
        try:
            with open(path, "rb") as f:
                data = base64.b64encode(f.read()).decode()
            content.append({"type": "image", "source": {"type": "base64", "media_type": media_type, "data": data}})
        except OSError:
            log.warning("Could not read image: %s", path)
    msg = {"type": "user", "message": {"role": "user", "content": content}}
    return json.dumps(msg)


async def run_claude(
    prompt: str,
    session_id: str | None = None,
    cwd: str | None = None,
    task_id: str | None = None,
    step_id: str | None = None,
    image_paths: list[str] | None = None,
) -> tuple[bool, str, str | None]:
    has_images = bool(image_paths)
    if has_images:
        args = [
            CLAUDE_PATH, "-p",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--dangerously-skip-permissions",
            "--system-prompt", SYSTEM_PROMPT,
        ]
    else:
        args = [
            CLAUDE_PATH, "-p", prompt,
            "--output-format", "stream-json",
            "--dangerously-skip-permissions",
            "--system-prompt", SYSTEM_PROMPT,
        ]
    if session_id:
        args.extend(["--resume", session_id])
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE if has_images else None,
            cwd=cwd or None,
        )
        if has_images:
            stdin_data = _build_stream_input(prompt, image_paths).encode() + b"\n"
            proc.stdin.write(stdin_data)
            proc.stdin.close()
        if step_id:
            _running_procs[step_id] = proc

        sid = session_id
        raw_events: list[dict] = []
        start_time = time.monotonic()
        prev_block_count = 0

        async def stream_stdout() -> None:
            nonlocal sid, prev_block_count
            while True:
                if time.monotonic() - start_time > STEP_TOTAL_TIMEOUT:
                    raise asyncio.TimeoutError()
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=STEP_IDLE_TIMEOUT)
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    ev = json.loads(text)
                    raw_events.append(ev)

                    if ev.get("type") == "system" and ev.get("session_id"):
                        sid = ev["session_id"]
                    if ev.get("type") == "result" and ev.get("session_id"):
                        sid = ev["session_id"]

                    if not (task_id and step_id):
                        continue

                    if ev.get("type") == "assistant":
                        blocks = ev.get("message", {}).get("content", [])
                        new_blocks = blocks[prev_block_count:]
                        prev_block_count = len(blocks)
                        if not new_blocks:
                            continue
                        delta_ev = {**ev, "message": {"content": new_blocks}}
                        cleaned_ev = _clean_single_event(delta_ev)
                    else:
                        if ev.get("type") != "assistant":
                            prev_block_count = 0
                        cleaned_ev = _clean_single_event(ev)

                    if cleaned_ev:
                        await broadcast_status({
                            "type": "step_log",
                            "task_id": task_id,
                            "step_id": step_id,
                            "event": cleaned_ev,
                        })
                except json.JSONDecodeError:
                    pass

        try:
            await stream_stdout()
        except asyncio.TimeoutError:
            elapsed = int(time.monotonic() - start_time)
            log.warning("Claude timed out after %ds", elapsed)
            proc.kill()
            return False, f"Timed out after {elapsed}s (idle={STEP_IDLE_TIMEOUT}s, max={STEP_TOTAL_TIMEOUT}s)", sid
        finally:
            if step_id:
                _running_procs.pop(step_id, None)

        await proc.wait()
        success = proc.returncode == 0

        if sid and cwd:
            _fix_session_index(sid, cwd)

        if not success and not raw_events:
            stderr = await proc.stderr.read()
            return False, stderr.decode("utf-8", errors="replace")[:50_000], sid

        cleaned = _clean_output(json.dumps(raw_events))
        if len(cleaned) > 50_000:
            cleaned = _truncate_json_events(cleaned, 50_000)
        return success, cleaned, sid
    except FileNotFoundError:
        return False, f"claude CLI not found (looked at: {CLAUDE_PATH})", None


def _clean_output(output: str) -> str:
    try:
        data = json.loads(output)
        if not isinstance(data, list):
            return output
        cleaned = []
        prev_block_count = 0
        for ev in data:
            ts = ev.get("timestamp")
            t = ev.get("type")
            if t == "system" and ev.get("subtype") == "init":
                prev_block_count = 0
                cleaned.append({
                    "type": "system", "subtype": "init", "timestamp": ts,
                    "cwd": ev.get("cwd"),
                    "session_id": ev.get("session_id"),
                    "model": ev.get("model"),
                })
            elif t == "assistant":
                msg = ev.get("message", {})
                content = msg.get("content", [])
                new_blocks = content[prev_block_count:]
                prev_block_count = len(content)
                if not new_blocks:
                    continue
                slim_content = []
                for block in new_blocks:
                    if block.get("type") == "text":
                        slim_content.append({"type": "text", "text": block.get("text", "")})
                    elif block.get("type") == "tool_use":
                        inp = block.get("input", {})
                        slim_input = {}
                        for k, v in (inp.items() if isinstance(inp, dict) else []):
                            sv = str(v)
                            slim_input[k] = sv[:200] if len(sv) > 200 else v
                        slim_content.append({"type": "tool_use", "name": block.get("name"), "input": slim_input})
                    elif block.get("type") == "thinking":
                        continue
                if slim_content:
                    cleaned.append({"type": "assistant", "timestamp": ts, "message": {"content": slim_content}})
            elif t == "tool_result":
                prev_block_count = 0
                content = ev.get("content", "")
                text = content if isinstance(content, str) else json.dumps(content)
                if len(text) > 500:
                    text = text[:500] + "…"
                cleaned.append({"type": "tool_result", "timestamp": ts, "content": text})
            elif t == "result":
                prev_block_count = 0
                cleaned.append({
                    "type": "result", "subtype": ev.get("subtype"), "timestamp": ts,
                    "result": (ev.get("result") or "")[:2000],
                    "session_id": ev.get("session_id"),
                    "duration_ms": ev.get("duration_ms"),
                    "num_turns": ev.get("num_turns"),
                    "total_cost_usd": ev.get("total_cost_usd"),
                })
        return json.dumps(cleaned)
    except (json.JSONDecodeError, TypeError):
        return output


def _truncate_json_events(output: str, max_len: int) -> str:
    """Drop middle events until serialised JSON fits in max_len."""
    try:
        events = json.loads(output)
        if not isinstance(events, list) or len(events) <= 2:
            return output[:max_len]
        while len(json.dumps(events)) > max_len and len(events) > 2:
            events.pop(-2)
        return json.dumps(events)
    except (json.JSONDecodeError, TypeError):
        return output[:max_len]


def _clean_single_event(ev: dict) -> dict | None:
    ts = ev.get("timestamp")
    t = ev.get("type")
    if t == "system" and ev.get("subtype") == "init":
        return {"type": "system", "subtype": "init", "timestamp": ts, "cwd": ev.get("cwd"), "session_id": ev.get("session_id"), "model": ev.get("model")}
    if t == "assistant":
        msg = ev.get("message", {})
        content = msg.get("content", [])
        slim = []
        for block in content:
            if block.get("type") == "text":
                slim.append({"type": "text", "text": block.get("text", "")})
            elif block.get("type") == "tool_use":
                inp = block.get("input", {})
                slim_inp = {k: str(v)[:200] for k, v in (inp.items() if isinstance(inp, dict) else [])}
                slim.append({"type": "tool_use", "name": block.get("name"), "input": slim_inp})
        return {"type": "assistant", "timestamp": ts, "message": {"content": slim}} if slim else None
    if t == "tool_result":
        content = ev.get("content", "")
        text = content if isinstance(content, str) else json.dumps(content)
        return {"type": "tool_result", "timestamp": ts, "content": text[:500]}
    if t == "result":
        return {"type": "result", "subtype": ev.get("subtype"), "timestamp": ts, "result": (ev.get("result") or "")[:2000], "session_id": ev.get("session_id"), "duration_ms": ev.get("duration_ms"), "num_turns": ev.get("num_turns"), "total_cost_usd": ev.get("total_cost_usd")}
    return None


def _detect_input_needed(output: str) -> str | None:
    try:
        data = json.loads(output)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("type") == "result":
                    result = item.get("result", "")
                    if result and result.rstrip().endswith("?"):
                        return result
        return None
    except (json.JSONDecodeError, TypeError):
        if output.rstrip().endswith("?"):
            return output.strip()
        return None


ATTACHMENTS_CACHE = os.path.expanduser("~/.taskrunner/attachments")


def _download_attachment(api_base: str, token: str, att: dict) -> str | None:
    path = att.get("path", "")
    m = re.search(r"attachments/([^/]+)/(.+)$", path)
    if not m:
        return None
    file_id, filename = m.group(1), m.group(2)
    local_dir = os.path.join(ATTACHMENTS_CACHE, file_id)
    local_path = os.path.join(local_dir, filename)
    if os.path.isfile(local_path):
        return local_path
    url = f"{api_base}/api/tasks/attachments/{file_id}/{filename}"
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


def build_prompt(step: dict, local_paths: list[str] | None = None, has_images: bool = False) -> str:
    prompt = f"Task: {step['task_title']}\n"
    if step["task_description"]:
        prompt += f"Description: {step['task_description']}\n"
    if local_paths:
        prompt += "\nAttached files (read with the Read tool):\n"
        for p in local_paths:
            prompt += f"  - {p}\n"
    if has_images:
        prompt += "\nScreenshots are attached as images in this message.\n"
    prompt += f"\nStep: {step['step_name']}\n"
    if step["step_note"]:
        prompt += f"Instructions: {step['step_note']}\n"
    return prompt


# ── Pipeline step processing ─────────────────────────────────────────────


async def process_step(api_base: str, token: str, step: dict) -> None:
    task_id = step["task_id"]
    step_id = step["step_id"]
    step_name = step["step_name"]
    task_title = step["task_title"]
    session_id = step.get("session_id")

    log.info("Running: %s / %s [%s]", task_title, step_name, step_id[:8])
    await broadcast_status({"type": "step_started", "task_id": task_id, "step_id": step_id, "step_name": step_name})

    local_paths: list[str] = []
    local_images: list[str] = []
    for att in step.get("attachments") or []:
        lp = _download_attachment(api_base, token, att)
        if lp:
            ext = os.path.splitext(lp)[1].lower()
            if ext in IMAGE_EXTS:
                local_images.append(lp)
            else:
                local_paths.append(lp)

    if session_id:
        prompt = f"Next step: {step_name}"
        if step.get("step_note"):
            prompt += f"\nInstructions: {step['step_note']}"
    else:
        prompt = build_prompt(step, local_paths or None, has_images=bool(local_images))

    working_dir = step.get("working_dir")
    success, output, sid = await run_claude(prompt, session_id, working_dir, task_id, step_id, local_images or None)

    question = _detect_input_needed(output) if success else None
    if question:
        log.info("  %s: needs input — %s", step_name, question[:80])
        _api_post(f"{api_base}/api/tasks/{task_id}/steps/{step_id}/input-needed", token, {"question": question, "session_id": sid})
        await broadcast_status({"type": "step_input", "task_id": task_id, "step_id": step_id, "question": question[:200]})
    else:
        status_str = "passed" if success else "failed"
        log.info("  %s: %s (%d chars)", step_name, status_str, len(output))
        _api_post(f"{api_base}/api/tasks/{task_id}/steps/{step_id}/complete", token, {"success": success, "output": output, "session_id": sid})
        await broadcast_status({"type": "step_completed", "task_id": task_id, "step_id": step_id, "status": status_str})


async def process_reply(api_base: str, token: str, reply: dict) -> None:
    task_id = reply["task_id"]
    step_id = reply["step_id"]
    session_id = reply.get("session_id")
    message = reply["message"]

    log.info("Resuming step %s with reply: %s", step_id[:8], message[:60])

    if not session_id:
        _api_post(f"{api_base}/api/tasks/{task_id}/steps/{step_id}/complete", token, {"success": False, "output": "No session to resume"})
        return

    success, output, sid = await run_claude(message, session_id, task_id=task_id, step_id=step_id)
    question = _detect_input_needed(output) if success else None

    if question:
        _api_post(f"{api_base}/api/tasks/{task_id}/steps/{step_id}/input-needed", token, {"question": question, "session_id": sid or session_id})
        await broadcast_status({"type": "step_input", "task_id": task_id, "step_id": step_id, "question": question[:200]})
    else:
        status_str = "passed" if success else "failed"
        _api_post(f"{api_base}/api/tasks/{task_id}/steps/{step_id}/complete", token, {"success": success, "output": output, "session_id": sid or session_id})
        await broadcast_status({"type": "step_completed", "task_id": task_id, "step_id": step_id, "status": status_str})


async def create_worktree(api_base: str, token: str, wt: dict) -> None:
    task_id = wt["task_id"]
    ticket_id = wt["ticket_id"]
    repo_path = wt["repo_path"]

    log.info("Creating worktree for %s in %s", ticket_id, repo_path)
    try:
        proc = await asyncio.create_subprocess_exec(
            "bash", WORKTREE_SCRIPT, ticket_id.lower(),
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
            _api_post(f"{api_base}/api/tasks/{task_id}/worktree-done", token, {"worktree_path": worktree_path})
        else:
            log.error("  Worktree failed (rc=%d): %s", proc.returncode, stderr.decode()[:200])
    except Exception as e:
        log.error("  Worktree error: %s", e)


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
    """Open a pty and stream it to the backend via a dedicated WebSocket."""
    sid = cmd["sid"]
    cwd = cmd.get("cwd") or os.path.expanduser("~")
    cols = cmd.get("cols", 120)
    rows = cmd.get("rows", 30)
    shell = os.environ.get("SHELL", "/bin/zsh")

    url = f"{ws_base}/ws/worker/terminal/{sid}?token={urllib.parse.quote(token)}"
    log.info("Opening terminal session %s (cwd=%s)", sid[:8], cwd)

    try:
        async with connect(url, max_size=2**20) as ws:
            master_fd, slave_fd = pty.openpty()
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)

            env = os.environ.copy()
            env["TERM"] = "xterm-256color"
            env["COLUMNS"] = str(cols)
            env["LINES"] = str(rows)

            proc = subprocess.Popen(
                [shell, "-l"],
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
    url = f"{ws_base}/ws/worker?token={urllib.parse.quote(token)}"
    log.info("Connecting to backend: %s", ws_base)

    async for ws in connect(url, ping_interval=30, ping_timeout=10, max_size=2**20):
        global _control_ws
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
                        killed = 0
                        for sid, proc in list(_running_procs.items()):
                            try:
                                proc.kill()
                                killed += 1
                            except ProcessLookupError:
                                pass
                        log.info("Stop task %s: killed %d process(es)", task_id[:8], killed)
                except json.JSONDecodeError:
                    pass
        except websockets.ConnectionClosed:
            pass
        _control_ws = None
        log.warning("Backend connection lost, reconnecting...")


# ── Polling loop ─────────────────────────────────────────────────────────


async def poll_loop(api_base: str, token: str, poll_interval: float) -> None:
    log.info("Polling %s every %.0fs", api_base, poll_interval)
    active: set[str] = set()

    while True:
        # Worktrees
        worktrees = _api_get(f"{api_base}/api/tasks/worktrees/pending", token) or []
        for wt in worktrees:
            if wt["task_id"] not in active:
                active.add(wt["task_id"])
                await create_worktree(api_base, token, wt)
                active.discard(wt["task_id"])

        # Steps
        steps = _api_get(f"{api_base}/api/tasks/steps/pending", token) or []
        new_steps = [s for s in steps if s["step_id"] not in active]
        if new_steps:
            log.info("Found %d pending step(s)", len(new_steps))
            for step in new_steps:
                active.add(step["step_id"])
            await asyncio.gather(*(process_step(api_base, token, s) for s in new_steps))
            for step in new_steps:
                active.discard(step["step_id"])

        # Replies
        replies = _api_get(f"{api_base}/api/tasks/steps/replies", token) or []
        new_replies = [r for r in replies if r["step_id"] not in active]
        if new_replies:
            log.info("Found %d reply(ies)", len(new_replies))
            for r in new_replies:
                active.add(r["step_id"])
            await asyncio.gather(*(process_reply(api_base, token, r) for r in new_replies))
            for r in new_replies:
                active.discard(r["step_id"])

        # Heartbeat
        await broadcast_status({"type": "heartbeat"})
        await asyncio.sleep(poll_interval)


# ── Main ─────────────────────────────────────────────────────────────────


async def main(api_base: str, token: str, poll_interval: float) -> None:
    ws_base = api_base.replace("https://", "wss://").replace("http://", "ws://")
    log.info("Worker starting (reverse tunnel mode)")
    log.info("  API:    %s", api_base)
    log.info("  WS:     %s", ws_base)
    log.info("  Claude: %s", CLAUDE_PATH)

    await asyncio.gather(
        control_loop(ws_base, token),
        poll_loop(api_base, token, poll_interval),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Taskrunner worker (reverse tunnel)")
    parser.add_argument("--api", default="https://taskrunner.dimash.dev", help="Backend API base URL")
    parser.add_argument("--token", default=os.environ.get("TASKRUNNER_TOKEN", ""), help="Auth token")
    parser.add_argument("--poll-interval", type=float, default=3, help="Seconds between polls")
    args = parser.parse_args()

    if not args.token:
        parser.error("--token is required (or set TASKRUNNER_TOKEN env var)")

    try:
        asyncio.run(main(args.api, args.token, args.poll_interval))
    except KeyboardInterrupt:
        log.info("Worker stopped")

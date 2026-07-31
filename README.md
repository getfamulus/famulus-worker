# famulus-worker

The machine-side half of [Famulus](https://github.com/getfamulus/famulus). It runs
Claude Code sessions on **your** computer, where your repositories, your tooling and
your `claude` login already live, and reports progress back to the Famulus backend.

Connections are **outbound only**. The worker dials the backend over WebSocket, so your
machine needs no open ports, no port forwarding and no public IP.

```
┌─────────────┐   outbound ws    ┌──────────────┐
│  worker     │ ───────────────► │   backend    │ ◄──── browser
│  (your Mac) │ ◄─────────────── │  (anywhere)  │
└─────┬───────┘   tasks, replies └──────────────┘
      │
      ▼  tmux session per task
   `claude` ── writes result JSON ── picked up by the worker
```

---

## ⚠️ Read this before installing

The worker starts `claude` with **`--permission-mode auto`** and pre-accepts the
workspace trust prompt, because a pipeline can't answer interactive dialogs. In
practice that means:

- Claude will **read, write and delete files** in the working directory you configure,
  and **run shell commands** there, without asking for confirmation.
- Task titles, descriptions and imported ticket text are fed into that session as
  instructions. **Treat anything you import as untrusted input.** A hostile ticket
  description is a prompt-injection vector that can reach your shell.
- The backend can ask the worker to open an **interactive terminal** on this machine,
  relayed to whoever is authenticated to the web UI.

Run it against directories you're willing to let an agent modify, with a backend only
you can reach. See [SECURITY.md](https://github.com/getfamulus/famulus-worker/blob/main/SECURITY.md) for the full trust model.

---

## Requirements

- Python 3.12+
- [`tmux`](https://github.com/tmux/tmux) on `PATH`
- [Claude Code](https://claude.com/claude-code) (`claude`) on `PATH`, already signed in
- A reachable [Famulus backend](https://github.com/getfamulus/famulus)

## Install

```bash
uv tool install famulus-worker
```

<details>
<summary>Other installation methods</summary>

```bash
pipx install famulus-worker          # isolated, also fine
pip install famulus-worker           # into the current environment

# from a checkout
git clone https://github.com/getfamulus/famulus-worker && cd famulus-worker
uv sync && uv run famulus-worker --help
```
</details>

## Run

```bash
famulus-worker --api http://localhost:8000 --worker-token "$FAMULUS_WORKER_TOKEN"
```

The token has to match the backend's `FAMULUS_WORKER_TOKEN`, or `FAMULUS_AUTH_TOKEN` if
you didn't configure a separate worker credential. On success you'll see:

```
Worker starting (reverse tunnel mode)
  API:    http://localhost:8000
  Claude: /opt/homebrew/bin/claude
Connected to backend relay
```

### Options

| Flag | Env | Default | Description |
|---|---|---|---|
| `--api` | `FAMULUS_API` | `http://localhost:8000` | Backend base URL |
| `--worker-token` | `FAMULUS_WORKER_TOKEN` | none | Worker credential (preferred) |
| `--token` | `FAMULUS_TOKEN` | none | Fallback credential if no worker token is set |
| `--poll-interval` | none | `3` | Seconds between task polls |

### Keeping it running

<details>
<summary>macOS launchd example</summary>

Save to `~/Library/LaunchAgents/dev.famulus.worker.plist`, then
`launchctl load ~/Library/LaunchAgents/dev.famulus.worker.plist`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>dev.famulus.worker</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/you/.local/bin/famulus-worker</string>
        <string>--api</string><string>https://famulus.example.com</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict><key>FAMULUS_WORKER_TOKEN</key><string>replace-me</string></dict>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
</dict>
</plist>
```
</details>

## How it works

1. **Poll.** Ask the backend for steps whose status is `running`, grouped by stage.
2. **Session.** Ensure a detached tmux session per task (`tr-<taskid>`) running one
   persistent `claude` conversation keyed to the task id, so later stages keep context.
3. **Dispatch.** Paste a stage prompt into the session via a tmux paste-buffer, never a
   shell string, so task text can't escape into your shell. Claude is asked to fan out
   one subagent per parallel job.
4. **Collect.** Claude writes `{"status","output"}` JSON per job into
   `<working_dir>/.tr/<task_id>/`. The worker posts each result and deletes the file.
5. **Relay.** On request, open a PTY (attaching to the task's tmux session when there is
   one) and stream it to the browser through the backend.

State lives in `~/.famulus/`: pane logs, session and dispatch markers, attachment cache.
Those markers are what make a restart adopt in-flight work instead of re-running it.

### Worktrees (optional)

If a task asks for a git worktree, the worker runs
`~/.claude/utils/worktree-new.sh <ticket-id>` in the repository and expects it to print
`WORKTREE_PATH=<path>`. Supply your own script at that path. There's a reference
implementation in [`examples/worktree-new.sh`](https://github.com/getfamulus/famulus-worker/blob/main/examples/worktree-new.sh).

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Backend closed during auth, reconnecting…` | Token mismatch. It must equal the backend's `FAMULUS_WORKER_TOKEN` |
| `claude cannot run: Login expired` | The CLI's login lapsed. Run `claude` on this machine, sign in, then re-run the task |
| `Session … did not report ready` | `claude` was slow or isn't signed in. Run `claude` once by hand |
| Steps stay "running" forever | No `tmux`, or Claude never wrote a result file. Open the live terminal to look |
| `Worktree failed` | Your `worktree-new.sh` is missing or didn't print `WORKTREE_PATH=` |

## Development

```bash
uv sync
uv run famulus-worker --help
uv build
```

## License

MIT, see [LICENSE](https://github.com/getfamulus/famulus-worker/blob/main/LICENSE).

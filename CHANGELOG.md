# Changelog

Notable changes to `famulus-worker`.

## v0.1.0 - 2026-07-30

First public release, extracted from the Famulus monorepo and packaged as an
installable CLI.

- `famulus-worker` console entry point; `uv tool install famulus-worker`
- Backend URL via `--api` or `FAMULUS_API`; credential via `--worker-token` /
  `FAMULUS_WORKER_TOKEN`, falling back to `--token` / `FAMULUS_TOKEN`
- Ships a reference `examples/worktree-new.sh` so the worktree feature no longer
  depends on an undocumented private script

### Security

- Authenticates both WebSocket channels with a first-message handshake instead of a
  query-string token, which reverse proxies write to their access logs
- Attachment downloads are path-containment checked, closing a traversal that could
  write outside the cache directory
- Task text still reaches Claude through a tmux paste-buffer over stdin and never
  through a shell string; all shell-facing values stay quoted

### Reliability

- HTTP calls run off the event loop, so a slow backend can no longer stall the control
  channel, live terminals or heartbeats
- Each poll iteration is isolated and both loops are supervised, so one malformed
  response cannot take the process down
- Dispatched stages are recorded on disk, so a restart adopts in-flight work rather than
  running it a second time
- A step fails promptly when its tmux session has gone, or when `claude` is signed out,
  instead of waiting out the hour-long timeout
- A terminal whose working directory no longer exists falls back to the home directory
  with a visible notice rather than closing silently

State lives in `~/.famulus/`. Upgrading from the pre-rename layout: `mv ~/.taskrunner
~/.famulus`.

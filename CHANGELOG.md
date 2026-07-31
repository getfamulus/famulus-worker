# Changelog

Notable changes to `famulus-worker`.

## v0.1.2 - 2026-07-31

### Fixed

- **A result is now tied to the execution that produced it.** Each dispatched
  step carries a `run_token` from the backend, echoed back with the result. If a
  step is stopped and restarted, a late result from the previous run is dropped
  instead of being applied to the new one — the backend's status check could not
  tell those apart, because a restarted step is `running` again.

  Requires a backend on v0.2.4 or later. Against an older backend the token is
  simply ignored, so this release is safe to install either way.

## v0.1.1 - 2026-07-31

### Security

- **Fixed a path traversal in attachment downloads.** The containment check
  compared the target against a directory built from the untrusted path, so once
  that directory had itself escaped, the check passed. An attachment path of the
  form `attachments/../../x.txt` wrote outside the cache directory, anywhere the
  worker's user could write. Attachment metadata can originate from an imported
  ticket, which the trust model treats as untrusted. Containment is now checked
  against the cache root, and the file id is rejected if it is `.` or `..`.

### Added

- A test suite — 54 tests covering attachment path containment, the stage prompt
  builder, result collection, the blocked-session detector and dispatch markers.
  CI runs them on Python 3.12 and 3.13.

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

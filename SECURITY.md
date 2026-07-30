# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security problems. Report privately via
[GitHub Security Advisories](https://github.com/getfamulus/famulus-worker/security/advisories/new).

Expect an acknowledgement within a few days. This is a small volunteer project, so
please allow reasonable time for a fix before public disclosure.

## Trust model — please read

Famulus is a tool for letting an AI agent work autonomously on your machine. That is
its purpose, and it has direct security consequences you should understand before
running it.

### What the worker can do on your machine

- Starts `claude` with **`--permission-mode auto`** and pre-accepts the workspace trust
  prompt (an unattended pipeline cannot answer dialogs). Claude therefore reads, writes
  and deletes files, and runs shell commands, in the configured working directory
  **without per-action confirmation**.
- Runs as **your user account**, with your environment, your credentials and your
  filesystem permissions. It is not sandboxed.
- Can open an **interactive shell** on this machine when the backend asks, relayed to
  whoever is authenticated to the web UI.

### Prompt injection is the main risk

Task titles, descriptions, step notes and text imported from Linear or Jira are passed
into the agent **as instructions**. Anyone who can create a task — or edit a ticket you
import — can influence what the agent does, up to running commands.

Task text is injected through a tmux paste-buffer over stdin, never interpolated into a
shell command, so it cannot *directly* escape into your shell. The exposure is at the
agent layer: Claude may be persuaded to run something itself.

**Consequences:** treat imported ticket content as untrusted. Do not point the worker at
a repository whose issue tracker accepts input from people you would not hand a shell to.

### Recommendations

- Run the worker against project directories you are willing to let an agent modify —
  ideally a git worktree, so changes are reviewable and easy to discard.
- Never expose the backend to the public internet without authentication. Set
  `FAMULUS_AUTH_TOKEN`; the backend refuses to start without it unless you explicitly
  set `FAMULUS_ALLOW_NO_AUTH=1`.
- Use a **separate** `FAMULUS_WORKER_TOKEN` so a leaked browser token does not grant
  worker-level access.
- Keep credentials out of URLs. Famulus authenticates every WebSocket with a
  first-message handshake specifically so tokens do not appear in proxy logs.
- Prefer a private network or a tunnel with access control (for example Cloudflare
  Access) over a bare public origin.

### Out of scope

- The agent making changes you did not want inside its configured working directory —
  that is the tool working as designed. Use worktrees and review diffs.
- Anything requiring an attacker to already have your worker token or shell access.

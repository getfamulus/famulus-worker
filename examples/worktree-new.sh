#!/usr/bin/env bash
# Reference implementation of the worktree helper the Famulus worker invokes.
#
# Install as ~/.claude/utils/worktree-new.sh (or adapt to your own conventions).
# The worker runs it inside the repository with the ticket id as $1 and reads the
# created path from a line of the form:
#
#     WORKTREE_PATH=/absolute/path/to/worktree
#
# Anything else printed is ignored, so feel free to log freely.
set -euo pipefail

ticket="${1:?usage: worktree-new.sh <ticket-id>}"
branch="$ticket"

# Worktrees are created as a sibling of the repository: /path/repo -> /path/repo-worktrees/<ticket>
repo_root=$(git rev-parse --show-toplevel)
worktrees_dir="${WORKTREES_DIR:-${repo_root}-worktrees}"
target="${worktrees_dir}/${ticket}"

if [ -d "$target" ]; then
    echo "Worktree already exists, reusing it"
    echo "WORKTREE_PATH=${target}"
    exit 0
fi

default_branch=$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's|^origin/||' || echo main)
git fetch --quiet origin "$default_branch"

mkdir -p "$worktrees_dir"
# --no-track keeps an accidental push from landing on the source branch.
git worktree add --no-track -b "$branch" "$target" "origin/${default_branch}"

echo "WORKTREE_PATH=${target}"

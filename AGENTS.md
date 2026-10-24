# Repository Guidelines

## Repository Scope

- Jcode Desktop is in a separate repository.

## TUI style

- UI chrome must not wrap: startup headers, account rows, status labels, and
  other compact UI rows should fit their available terminal-cell width or
  truncate with an ellipsis. Count Unicode display cells, not characters.
  Preserve one logical row per visual row, including at narrow widths.
  Chat prose and other intentionally multiline content may wrap normally.
- Reuse the info widget's limit-bar renderer for usage indicators instead of
  introducing a different bar glyph, color, or used/remaining convention.

## Development Workflow

- **Use the user's Git identity** - Create commits with the configured
  `user.name` and `user.email`. Do not override them with `Jcode`, `Jcode agent`,
  or a fabricated agent email. Preserve existing contributor attribution when
  integrating work. If no identity is configured, ask rather than inventing one.
- **Welcome pull requests from everyone** - Review contributions on their merits,
  regardless of whether the author is a maintainer, an existing contributor, a
  first-time contributor, or an agent. Good PRs can be merged directly after review
  and validation. Do not require a maintainer-authored rewrite merely because of
  who submitted the change. See `CONTRIBUTING.md` for the contribution policy.
- **Prefer master/main** - Work directly on the repository's primary branch
  (`master` or `main`) by default. Do not create a branch or worktree merely to
  start a task. Only do so when the user explicitly requests it or isolation is
  genuinely necessary, and explain the concrete reason first.
- **Keep work scoped** - Preserve unrelated work. If already on a branch with
  ongoing changes, finish and integrate safely rather than switching blindly.
  When the user asks you to review or integrate a PR or branch, you may inspect,
  test, and integrate that contribution regardless of author status. Do not pull
  in unrelated branches or merge a PR without user authorization.

## Install Notes
- `~/.local/bin/jcode` is the launcher symlink used from `PATH`.
- `~/.jcode/builds/current/jcode` is the active local/source-build channel; self-dev builds and `scripts/install_release.sh` point the launcher here.
- `~/.jcode/builds/stable/jcode` is the stable release channel; `scripts/install.sh` installs this and points the launcher here.
- `~/.jcode/builds/versions/<version>/jcode` stores immutable binaries.
- `~/.jcode/builds/canary/jcode` still exists for canary/testing flows, but it is not the primary self-dev install path.
- On Windows, the equivalents are `%LOCALAPPDATA%\\jcode\\bin\\jcode.exe` for the launcher, `%LOCALAPPDATA%\\jcode\\builds\\stable\\jcode.exe` for stable, and `%LOCALAPPDATA%\\jcode\\builds\\versions\\<version>\\jcode.exe` for immutable installs; `scripts/install.ps1` currently installs the stable channel.
- Ensure `~/.local/bin` is **before** `~/.cargo/bin` in `PATH`.

## Making a freshly built binary live on this machine (fork-specific)

On this machine the installed `jcode` is a symlink into this repo's build output, so a
release build becomes live immediately:

```bash
# 1. Build (already live after this completes - see sanity check below)
cargo build --release --bin jcode

# 2. Verify which binary PATH resolves to and that it is our build
which jcode                      # expect /opt/homebrew/bin/jcode
readlink -f /opt/homebrew/bin/jcode   # expect /Users/talha/Documents/workspace/jcode/target/release/jcode
jcode version                    # git_hash must match `git rev-parse --short HEAD`

# 3. Restart the shared daemon so `jcode run` and running sessions pick it up
jcode server stop && jcode serve
```

Sanity check one-liner (run from the repo root; fails loudly if PATH points elsewhere):

```bash
cd /Users/talha/Documents/workspace/jcode
[ "$(readlink -f "$(which jcode)")" = "$PWD/target/release/jcode" ] \
  && echo OK: fork build is live \
  || echo MISMATCH: PATH jcode is not this repo's build
```

Expected output today: `OK: fork build is live`, with `jcode version`'s
`git_hash` matching the repo HEAD commit (it will be one commit behind if the
README/docs changed after the last rebuild; that is fine, only source changes
require a rebuild).

Notes:
- If `which jcode` does NOT resolve through this repo, re-point the launcher:
  `ln -sf "$PWD/target/release/jcode" /opt/homebrew/bin/jcode` (or the
  `~/.local/bin/jcode` launcher documented above, keeping it first in PATH).
- The daemon (`~/.jcode/builds/shared-server/jcode`) may pin an older copy; use
  `jcode self-dev --build` per the section above if runtime checks still show the
  old `git_hash` after a rebuild.
- `git_hash` in `jcode version` output is the ground truth: it must equal
  `git rev-parse --short HEAD` of this repo.

## Verifying a change at runtime

`cargo build` alone proves nothing about behavior. `jcode run` and interactive
sessions are served by the long-lived daemon at
`~/.jcode/builds/shared-server/jcode`, which is a symlink into
`~/.jcode/builds/versions/<version>/`. Until that symlink is repointed and the
daemon restarted (`jcode self-dev --build`), a freshly built binary is inert and
every runtime check silently measures the old code.

To test a change without disturbing the shared daemon or the caller's session,
run your build against its own socket:

```bash
cargo build --profile selfdev
./target/selfdev/jcode run --no-update --socket /run/user/1000/jcode-mytest.sock '<prompt>'
```

Two things that waste time otherwise:

- `crate::logging::info` writes to a log file, not stderr, so instrumenting a
  code path with it produces no visible output under `--trace`. Use `eprintln!`
  for throwaway diagnostics and delete it before committing.
- Confirm which binary you are actually inspecting. `strings` on
  `builds/shared-server/jcode` reads a 70-byte symlink, not a program; resolve it
  with `readlink -f` first.

#!/bin/sh
# Install repository git hooks from hooks/ into .git/hooks/.
set -e
ROOT=$(git rev-parse --show-toplevel)
for hook in "$ROOT"/hooks/*; do
    [ -f "$hook" ] || continue
    name=$(basename "$hook")
    cp "$hook" "$ROOT/.git/hooks/$name"
    chmod +x "$ROOT/.git/hooks/$name"
    echo "Installed $name"
done

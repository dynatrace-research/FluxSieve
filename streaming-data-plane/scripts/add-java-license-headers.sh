#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/add-java-license-headers.sh [path]
# Default path: kstreams/src

TARGET_DIR="${1:-kstreams/src}"
MARKER="Licensed under the terms in the repository LICENSE file."

if [[ ! -d "$TARGET_DIR" ]]; then
  echo "Target directory not found: $TARGET_DIR" >&2
  exit 1
fi

find "$TARGET_DIR" -type f -name "*.java" -print0 | while IFS= read -r -d '' file; do
  if grep -q "$MARKER" "$file"; then
    continue
  fi

  tmp_file="$(mktemp)"
  {
    printf '/*\n'
    printf ' * Copyright (c) 2026 Dynatrace LLC. All rights reserved.\n'
    printf ' * Licensed under the terms in the repository LICENSE file.\n'
    printf ' */\n\n'
    cat "$file"
  } > "$tmp_file"

  mv "$tmp_file" "$file"
  echo "Updated: $file"
done

echo "Done."
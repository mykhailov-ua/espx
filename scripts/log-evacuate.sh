#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${LOG_DIR:-/var/log/espx}"
STORAGE_BOX_HOST="${STORAGE_BOX_HOST:?STORAGE_BOX_HOST is required}"
STORAGE_BOX_USER="${STORAGE_BOX_USER:?STORAGE_BOX_USER is required}"
STORAGE_BOX_PATH="${STORAGE_BOX_PATH:-/}"
RSYNC_BWLIMIT="${RSYNC_BWLIMIT:-5000}"

mkdir -p "$LOG_DIR"

for file in "$LOG_DIR"/segment_*.log.zst.ready; do
	[ -e "$file" ] || continue
	base="${file%.ready}"
	evac_file="$base.evacuating"
	mv "$file" "$evac_file"
	remote_filename=$(basename "$base")
	if ! rsync -az -e "ssh -o StrictHostKeyChecking=no" --bwlimit="$RSYNC_BWLIMIT" "$evac_file" "$STORAGE_BOX_USER@$STORAGE_BOX_HOST:$STORAGE_BOX_PATH/$remote_filename"; then
		mv "$evac_file" "$file"
		exit 1
	fi
	rm "$evac_file"
done

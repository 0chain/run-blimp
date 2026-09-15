#!/usr/bin/env bash
# scratch_dir.sh — pick a working directory that actually has room.
#
# Everything that stages a dataset locally before uploading it was defaulting to
# the BOOT disk: the TPC-DS generator to /tmp/_sf<N>.duckdb and $HOME/.blimp_sf<N>,
# the mlperf leg to /var/tmp/mlperf-gen. On a cloud VM the boot disk is small and
# the data disk is the big one, so the bigger scale factors fill root and die
# mid-run. Reported from a fresh node (2026-09-15): the mlperf leg wrote ~33 GiB
# of scratch to /var/tmp, hit "No space left on device", and the whole suite
# aborted with "no leg produced numbers" — after the earlier legs had already
# produced good numbers.
#
# Two rules:
#   1. An explicit override always wins. The operator knows their box.
#   2. Otherwise pick the candidate mount with the most FREE space, and refuse
#      up front if nothing has enough — a clear message before the run beats
#      ENOSPC thirty minutes in.

# scratch_pick NEED_GB [OVERRIDE] [NAME]
#   Echoes a usable directory, or returns 1 having explained why not.
scratch_pick(){
  local need_gb="${1:-1}" override="${2:-}" name="${3:-scratch}"

  if [ -n "$override" ]; then
    mkdir -p "$override" 2>/dev/null || sudo mkdir -p "$override" 2>/dev/null || true
    printf '%s' "$override"; return 0
  fi

  # Candidates, widest-first: explicit data mounts, then the node's own data
  # paths, then the boot-disk fallbacks. A candidate counts only if it exists
  # and we can write to it.
  local c avail best="" best_avail=0
  for c in /data /data1 /data2 /mnt/data /mnt "$BLIMP_SCRATCH_ROOT" \
           /var/0chain /opt /var/tmp /tmp "$HOME"; do
    [ -n "$c" ] && [ -d "$c" ] || continue
    [ -w "$c" ] || sudo test -w "$c" 2>/dev/null || continue
    # -P: POSIX one-line output. -BG would round; -k and divide keeps it portable.
    avail=$(df -Pk "$c" 2>/dev/null | awk 'NR==2{print int($4/1048576)}')
    [ -n "$avail" ] || continue
    if [ "$avail" -gt "$best_avail" ]; then best_avail="$avail"; best="$c"; fi
  done

  if [ -z "$best" ]; then
    echo "scratch: no writable candidate directory found" >&2
    return 1
  fi
  if [ "$best_avail" -lt "$need_gb" ]; then
    echo "scratch: need ~${need_gb} GB for ${name}, but the roomiest writable" >&2
    echo "         mount is ${best} with only ${best_avail} GB free." >&2
    echo "         Attach a bigger disk, or point BLIMP_SCRATCH_ROOT (or the" >&2
    echo "         per-step override) at one that has room." >&2
    return 1
  fi

  local d="$best/.blimp-scratch/$name"
  mkdir -p "$d" 2>/dev/null || sudo mkdir -p "$d" 2>/dev/null || {
    echo "scratch: cannot create $d" >&2; return 1; }
  sudo chown -R "$(id -u)" "$best/.blimp-scratch" 2>/dev/null || true
  printf '%s' "$d"
}

# scratch_report DIR — one line saying where the space is going and how much is left.
scratch_report(){
  local d="$1" avail
  avail=$(df -Pk "$d" 2>/dev/null | awk 'NR==2{print int($4/1048576)}')
  echo "  scratch: $d (${avail:-?} GB free on $(df -P "$d" 2>/dev/null | awk 'NR==2{print $6}'))"
}

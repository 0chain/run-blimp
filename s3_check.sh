# s3_check.sh — sourced by blimp (--doctor) and standup_data.sh (before it
# generates anything). One place for "can this identity WRITE this bucket?".
#
# s3_write_check <bucket> [aws global args…] — PUT a small marker object
# (.blimp/write-check, overwritten each time; never read as data: it holds no
# parquet) with whatever credentials the caller's environment carries. Returns
# 0 when the PUT succeeds; otherwise prints the aws error's last line and
# returns 1. head-bucket alone passes on a read-only role, so it is not enough.
s3_write_check(){
  local b="$1"; shift
  local err
  err=$(printf 'blimp write check\n' | aws "$@" s3 cp - "s3://$b/.blimp/write-check" 2>&1 >/dev/null) && return 0
  printf '%s' "${err:-PutObject failed}" | grep -v '^$' | tail -1
  return 1
}

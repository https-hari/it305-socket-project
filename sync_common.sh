#!/bin/sh
# common/ is the single source of truth. The handout requires every Part*/ folder to be self-contained,
# so this copies common/ into each of them.   Usage:  ./sync_common.sh          (copy)
#                                                      ./sync_common.sh --check  (fail if any copy is stale)
cd "$(dirname "$0")" || exit 1
rc=0
for d in Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced; do
  if [ "$1" = "--check" ]; then
    diff -rq common "$d/common" >/dev/null 2>&1 || { echo "STALE: $d/common differs from common/"; rc=1; }
  else
    rm -rf "$d/common" && cp -r common "$d/common" && echo "synced $d/common"
  fi
done
[ "$1" = "--check" ] && [ $rc -eq 0 ] && echo "all copies in sync"
exit $rc

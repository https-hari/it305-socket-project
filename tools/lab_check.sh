#!/usr/bin/env bash
# Pre-flight check for lab day. Run it on EVERY computer before the experiment / viva.
#   tools/lab_check.sh                      checks tools, builds, runs a loopback self-test, prints this PC's IPs
#   tools/lab_check.sh <server_ip> [port]   additionally checks that this PC can open a TCP connection to the server
set -u
cd "$(dirname "$0")/.."
ok=1; pass() { echo "  [ok]   $*"; }; bad() { echo "  [FAIL] $*"; ok=0; }
echo "== tools"
for t in gcc make python3; do command -v $t >/dev/null && pass "$t: $($t --version 2>&1 | head -1)" || bad "$t not found"; done
[ "$(uname -s)" = Linux ] && pass "Linux ($(uname -r))" || bad "not Linux ($(uname -s)): the code needs Linux (sendfile, MSG_MORE); use Docker or a lab PC"
echo "== build"
if make >/tmp/it305_build.log 2>&1; then pass "make built all four parts"; else bad "make failed, see /tmp/it305_build.log"; tail -5 /tmp/it305_build.log; fi
for d in Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced; do [ -x $d/server ] && [ -x $d/client ] || bad "$d/server or $d/client missing"; done
echo "== loopback self-test"
tmp=$(mktemp -d); mkdir -p "$tmp/data/t1"; for i in 1 2 3 4 5; do head -c $((i * 40000)) /dev/urandom > "$tmp/data/t1/f$i.bin"; done
for d in Part1 Part2_Case2 Part2_Case2_Enhanced; do
  port=$((20000 + RANDOM % 20000)); args=(); [ $d != Part1 ] && args=(0)
  ./$d/server $port "$tmp/data" ${args[@]+"${args[@]}"} >/dev/null 2>&1 & sp=$!; sleep 0.5
  rm -rf "$tmp/out"; ./$d/client 127.0.0.1 $port t1 "$tmp/out" >/dev/null 2>&1
  kill -INT $sp 2>/dev/null; wait $sp 2>/dev/null
  diff -rq "$tmp/data/t1" "$tmp/out/t1" >/dev/null 2>&1 && pass "$d transfers files correctly on loopback" || bad "$d loopback transfer failed"
done
rm -rf "$tmp"
echo "== this computer's addresses (give the server's to the clients)"
if command -v hostname >/dev/null && hostname -I >/dev/null 2>&1; then echo "  $(hostname -I)"; else ip -4 addr 2>/dev/null | awk '/inet /{print "  " $2}'; fi
if [ $# -ge 1 ]; then
  port=${2:-5000}; echo "== reaching the server at $1:$port (start the server first)"
  if (exec 3<>/dev/tcp/$1/$port) 2>/dev/null; then pass "TCP connection to $1:$port works"
  else bad "cannot connect to $1:$port: is the server running there? firewall (sudo ufw allow $port/tcp)? same network/VLAN?"; fi
fi
echo; [ $ok = 1 ] && echo "READY" || { echo "NOT READY: fix the [FAIL] lines above"; exit 1; }

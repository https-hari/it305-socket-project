#!/usr/bin/env bash
# Rehearsal rig: three Linux containers that stand in for the three lab PCs
#   it305-server   it305-client1   it305-client2      (one Docker network, project mounted at /app, data at /data)
# Everything the lab experiment does -- start a server elsewhere, run clients on two other machines, collect the
# results -- runs the same way here, from one laptop.
#
#   tools/rig.sh up                       start the 3 containers and build the project
#   tools/rig.sh test                     run the 13 correctness checks inside the server container
#   tools/rig.sh shape 2 100              emulate the link: 2 ms one-way delay, 100 Mbit/s (needs tc/netem in Docker)
#   tools/rig.sh unshape                  remove the shaping
#   tools/rig.sh bench --topic elefante --client-counts 1,2,4 --failure-probabilities 0,0.0001
#   tools/rig.sh demo                     3-machine live demo (server on one, a client on each of the others)
#   tools/rig.sh shell server|client1|client2
#   tools/rig.sh down                     remove everything
# Environment: DATA=<dir containing the topic folders> (default ~/Desktop/it305_data/raw-img), IMAGE (default it305)
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE=${IMAGE:-it305}; NET=it305net
DATA=${DATA:-$HOME/Desktop/it305_data/raw-img}
NODES="it305-server it305-client1 it305-client2"

die() { echo "rig: $*" >&2; exit 1; }
case "${1:-help}" in
  up)
    [ -d "$DATA" ] || die "dataset folder not found: $DATA (set DATA=...)"
    docker image inspect "$IMAGE" >/dev/null 2>&1 || die "image '$IMAGE' not found: run  docker build -t $IMAGE .  first"
    docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
    for n in $NODES; do
      docker rm -f "$n" >/dev/null 2>&1 || true
      docker run -d --name "$n" --hostname "$n" --network "$NET" --cap-add NET_ADMIN \
        -v "$PWD":/app -v "$DATA":/data:ro "$IMAGE" sleep infinity >/dev/null
    done
    docker exec it305-server make -C /app
    echo; echo "rig is up: it305-server, it305-client1, it305-client2 (project at /app, topics at /data)." ;;
  test)   docker exec -t it305-server make -C /app test DATA=/data ;;
  shape)
    delay=${2:?usage: rig.sh shape <one-way delay ms> [rate Mbit/s]}; rate=${3:-0}
    args="delay ${delay}ms"; [ "$rate" != 0 ] && args="$args rate ${rate}mbit"
    for n in $NODES; do
      docker exec "$n" tc qdisc replace dev eth0 root netem $args \
        || die "tc/netem is not available in this Docker VM. Use tools/netem_proxy.py instead (see README, section 10)."
    done
    echo "link shaped: $args on every node's eth0 (RTT about $((2 * ${delay%.*})) ms)" ;;
  unshape) for n in $NODES; do docker exec "$n" tc qdisc del dev eth0 root 2>/dev/null || true; done; echo "shaping removed" ;;
  bench)
    shift
    topics=(); [ -d "$DATA" ] && topics=(--topics-root "$DATA")
    IT305_SSH="$PWD/tools/docker-ssh" python3 Results/benchmark.py ${topics[@]+"${topics[@]}"} \
      --server-host it305-server --server-ssh root@it305-server \
      --client-ssh root@it305-client1,root@it305-client2 --no-local-clients \
      --remote-dir /app --remote-topics /data --no-build "$@" ;;
  demo) shift; exec python3 tools/demo.py --docker "$@" ;;
  shell) docker exec -it "it305-${2:?usage: rig.sh shell server|client1|client2}" bash ;;
  down)
    for n in $NODES; do docker rm -f "$n" >/dev/null 2>&1 || true; done
    docker network rm "$NET" >/dev/null 2>&1 || true; echo "rig removed" ;;
  *) sed -n '2,17p' "$0" ;;
esac

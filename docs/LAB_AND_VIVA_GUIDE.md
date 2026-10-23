# IT305 — Lab & Viva Guide

Everything you need to understand the system, rehearse it alone, run it on the three lab computers and defend it in the viva.
All commands assume the project root as the current directory.

---

## 1. The project on one page

**The assignment.** A server holds *topics* (one folder of files each). A client says `GET <topic>` and receives every file of the
topic. Part I compares a single-threaded server with a thread-per-client server. Part II injects random connection failures and
compares three recovery strategies. Everything is plain TCP sockets in C.

```
 client                                              server (one thread per connection)
   │── CMD_GET "cane" ───────────────────────────────▶│  look the topic up (hash table built at start-up)
   │◀── FILE_HEADER (name,size,offset) ───────────────│  for every file, in sorted order:
   │◀── FILE_DATA (<=64 KB) … FILE_DATA ──────────────│     header, then 64 KB chunks
   │◀── END_OF_TOPIC ─────────────────────────────────│  connection closed
```

Every message is an **8-byte header** (`type`, `flags`, `status`, `payload length`, big-endian) followed by the payload. TCP is a byte
stream with no message boundaries, so the length field is what lets the receiver cut the stream back into messages.

| Case | Server | What the client does when the connection drops |
|---|---|---|
| **Part I** | single-threaded *or* thread-per-client, no faults | – |
| **Case 1** | multi-threaded + random connection resets | throws everything away and asks for the topic again from byte 0 |
| **Case 2** | same | keeps a **checkpoint** (topic, current file, byte offset), reconnects, sends `CMD_RESUME(file, offset)`; the server checks it and continues from that byte |
| **Enhanced** | same + `sendfile()` + 256 KB socket buffers | **K parallel connections**, each fetching whole files from a shared queue with several requests in flight (pipelined); a lost connection resumes only its own file |

**Fault injection.** The server tosses a biased coin (probability *p* from the command line) right after it sends each file header and
before each 64 KB chunk. If it fires, it sets `SO_LINGER{on,0}` and closes: the client sees a TCP **reset**, exactly like a crashed link.

**Where the code is.** `common/server_core.c` (server for all four cases, selected by a case id), `common/client_core.c` (all clients),
`common/io_helpers.c` (framing), `common/protocol.h` (opcodes). Each `PartX/server.c` and `client.c` is a few lines that pick the case.

---

## 2. Design decisions — and what we rejected

| Decision | Chosen | Rejected, and why |
|---|---|---|
| Message framing | fixed 8-byte binary header + length | newline-delimited text: the payload is binary images (would need escaping or Base64, +33 %) |
| Concurrency | thread per connection (the handout requires it for Part II); single-threaded = same code, run inline | `select`/`epoll`: fine technically, but the handout asks for a thread per client and threads keep the code simple |
| Where resume state lives | **client** checkpoint; server stays stateless between connections and just *validates* the offset | server session table: lost on a server restart, grows if clients never return, needs timeouts and cleanup |
| Failure model | probability per check; abortive close (RST) | clean `close()`: clients could tell it from a real failure, and queued data would still be delivered |
| Enhanced parallelism | **whole files** per connection | byte ranges of one file: reassembly and out-of-order writes for no gain on thousands of small images |
| Enhanced requests | **pipelined**: up to 8 requests in flight per connection (`IT305_ENH_WINDOW`) | stop-and-wait (one file, wait, next): costs one round trip per file; we measured it **slower than plain Case 2** as soon as there was any latency (see §3) |
| Durability | no `fsync` per chunk; checkpoint is one in-place 512-byte write | `fsync` per chunk: survives power loss but made throughput depend on the disk, not on the protocol. We survive a *process* crash (tested with `kill -9`), not an OS crash |
| Per-connection RNG seed | clock nanoseconds + pid + counter, mixed (splitmix64) | `time ^ pid ^ fd`: file-descriptor numbers repeat, so there were only a handful of distinct seeds and faults repeated |

---

## 3. What to expect from the experiments (so nothing surprises you in the viva)

* **Part I, single vs multi.** The multi-threaded server wins only when *one client cannot use all of the server's capacity* (slow
  client disk, slow per-connection path, server CPU/disk work to overlap). On a fast, idle gigabit LAN one client may already fill the
  link, and the two curves can be close. Say so; it is a correct observation, not a bug. The **first** run reads the dataset from disk;
  later runs hit the page cache. Warm up (`cat raw-img/*/* > /dev/null`) or discard run 1.
* **Case 1.** A transfer with *N* checks survives untouched with probability (1-p)^N. The largest Kaggle topic has about 9,700 checks,
  so p = 1e-4 gives ~38 % per attempt; p = 1e-3 gives ~0.006 % and Case 1 effectively never finishes. That collapse **is** the result.
* **Case 2.** The client never receives a byte twice, so client-side *redundant bytes* is 0. The cost shows up in **server-sent
  excess**: when the server resets, data still queued in the kernel is discarded and must be sent again. Plot both.
* **Enhanced.** Parallel connections help when *one connection is the bottleneck* (loss, high latency, per-flow limits, a slow
  receiver thread). On a clean LAN with K=1, pipelined Enhanced ≈ Case 2; the gain appears with K>1 only if a single flow is limited.
  Enhanced also loses less to resets: only a bounded window of data is in flight.

**Rehearsal numbers (one machine, software delay/rate emulation — NOT lab results; `tools/netem_proxy.py`):**

| Scenario (85 MB, 3,000-file topic) | Case 2 (1 connection) | Enhanced, stop-and-wait (old) | Enhanced, pipelined (now) |
|---|---|---|---|
| 1 ms one-way delay | 1.9 s | 4.5 s | 2.2 s |
| 5 ms one-way delay | 1.9 s | 10.5 s | 2.3 s |

| Each connection capped at 100 Mbit/s, 2 ms delay | time | speed-up vs Case 2 |
|---|---|---|
| Case 2 (1 connection) | 8.3 s | 1.0× |
| Enhanced K=1 | 8.7 s | 0.95× |
| Enhanced K=2 | 4.5 s | 1.9× |
| Enhanced K=4 | 2.4 s | 3.5× |
| Enhanced K=8 | 1.9 s (emulator's own ceiling) | 4.3× |

---

## 4. Rehearse alone: three containers on your Mac stand in for the three PCs

One-time: `docker build -t it305 .` (the image also contains `tc` for link shaping; rebuild once if you built it earlier).
Dataset: `~/Desktop/it305_data/raw-img` (override with `DATA=...`).

```bash
tools/rig.sh up                 # it305-server, it305-client1, it305-client2 on one Docker network, project built
tools/rig.sh test               # 17 correctness checks inside the server container
tools/rig.sh shape 2 100        # optional: emulate 2 ms one-way delay and a 100 Mbit/s link (tc/netem)
tools/rig.sh bench --topic elefante --client-counts 1,2,4 --failure-probabilities 0,0.0001,0.0002
tools/rig.sh unshape
tools/rig.sh shell client1      # a shell on a "client PC" to type the live commands of §6 yourself
tools/rig.sh down
```

`bench` drives the same code path as the real lab run: `benchmark.py` starts the server **on one container over a remote shell** and
the clients **on the other two**, then collects the results (`IT305_SSH=tools/docker-ssh` makes it use `docker exec` instead of ssh).
If `shape` says netem is unavailable in your Docker VM, use the software emulator instead — it needs no privileges:

```bash
python3 tools/netem_proxy.py --listen-port 6000 --target 127.0.0.1:5000 --delay-ms 2 --rate-mbit 100   # clients use port 6000
```

For a narrated one-machine walk-through of all four ideas (≈2 minutes):
`docker exec -it it305-server python3 /app/tools/demo.py --topics-root /data`  (or `python3 tools/demo.py --topics-root <dir>` on Linux).

> Caveat: Docker Desktop on a laptop shares one CPU, one disk and a virtual network. Use it to check that everything works and to
> learn how the numbers move. **Take the report's measurements on the lab computers.**

---

## 5. Lab day: three computers

### 5.1 Before anything else (on each PC, 5 minutes)

```bash
git clone https://github.com/https-hari/it305-socket-project.git && cd it305-socket-project   # or copy the folder
tools/lab_check.sh                     # tools present? builds? loopback transfer works? prints this PC's IP
```

Pick one PC as the **server** and note its IP (`hostname -I`). On each client PC check it can reach the server:
`tools/lab_check.sh <server_ip> 5000` (start any server first, see 5.2). If it cannot connect: same network? `sudo ufw allow 5000/tcp`
(Ubuntu firewall) on the server PC; some lab networks block ports, then try another one.
Put the dataset on the **server PC only** (the clients do not need it).

### 5.2 Quick manual run (3 terminals — also the core of the viva demo)

```bash
# PC1 (server)                                   # failure probability 0.0002 for the resume cases
cd Part2_Case2 && ./server 5000 /path/to/raw-img 0.0002
# PC2 (client)                                   # PC3 the same with another topic, at the same time
cd Part2_Case2 && ./client <PC1_IP> 5000 cane /tmp/out_pc2
```

You should see `[FAULT_INJECTION] drop file=… offset=…` lines on PC1, `Connection interrupted; resuming … at offset …` lines on the
client, and finally `Transfer completed` plus a `RESULT,…` line. Compare the downloaded folder with the source:
`diff -r /path/to/raw-img/cane /tmp/out_pc2/cane` (no output = identical).

### 5.3 The full measured experiment, driven from one PC

Needs: the project built at the **same path** on all three PCs, passwordless ssh from the PC you run this on to the other two
(`ssh-keygen`, then `ssh-copy-id user@host`), the dataset on the server PC.

```bash
python3 Results/benchmark.py --topic cane --client-counts 1,2,4,8,16 \
   --failure-probabilities 0,0.00001,0.0001,0.0002 --repeat 3 \
   --server-host <PC1_IP> --server-ssh user@<PC1_IP> --client-ssh user@<PC3_IP> \
   --remote-dir /path/to/project --remote-topics /path/to/raw-img --no-build
```

Clients are spread round-robin over this PC and the `--client-ssh` machines (add `--no-local-clients` if this PC should only
orchestrate). No ssh allowed? Add `--manual-server`: the script prints the exact server command for each scenario and waits.
Add `--server-data-path buffered` in a second run for the sendfile-vs-buffered comparison, and vary `--parallel-connections` for K.
Results land in `Results/raw_metrics.csv` and the graphs next to it (`--graphs-only` redraws them).

**Server-side numbers** (the handout's "time to send data" and "bytes sent"): `server_window_sec` is the span from the first request to
the last finished reply across all clients, `server_aggregate_throughput_Bps` = bytes the server wrote / that window.

### 5.4 Checklist before you leave the lab
`Results/raw_metrics.csv` has rows for every case · graphs exist · you noted the link speed (`ethtool` or ask) and the PCs' CPUs ·
you copied `Results/` off the PCs · screenshots of one live run (server log + client log).

---

## 6. Viva demo script (≈8 minutes)

1. **(30 s) Architecture.** Show §1's diagram. "8-byte header, 64 KB chunks, thread per client, client-side checkpoint."
2. **(1 min) Part I.** PC1: `cd Part1 && ./server 5000 /data multi`. PC2 and PC3 run `./client <ip> 5000 cane /tmp/o` together.
   `diff -r` the output. Then restart the server with `single` and repeat: "single serves one client at a time."
3. **(2 min) Case 1 vs Case 2 — the key contrast.** Use the **same** probability on both, e.g. 0.0001 on `cane`.
   Case 1 prints `Connection interrupted; restarting topic from byte 0`; Case 2 prints `resuming … at offset …`. Compare the
   `RESULT` lines: `received_payload_bytes` vs `useful_payload_bytes` — Case 1's redundant bytes are huge, Case 2's are 0.
4. **(1 min) Crash safety.** On a client PC start Case 2 on a big topic, `kill -9` the client part-way, run the same command again:
   it resumes from the checkpoint (`.checkpoint.tmp`) and receives only the rest.
5. **(2 min) Enhanced.** `./server 5000 /data 0.0001` and `./client <ip> 5000 cane /tmp/o 4`; explain K connections, pipelining, sendfile.
   Show the graphs: throughput vs failure probability, overhead vs failure probability.
6. **(1 min) Honest limits.** "On our LAN a single connection already fills the link, so Enhanced's gain is X; it would grow with
   latency or loss. No checksum, so a *corrupt* checkpoint with a valid offset would not be detected."

Backup if the network misbehaves: `python3 tools/demo.py --topics-root /path/to/raw-img` runs the whole story on one machine.

---

## 7. Questions you may be asked

**Why TCP, and why do you need a header?** Files must arrive intact and in order, which TCP gives. TCP delivers a byte stream, not
messages, so each message carries its length.

**What does a thread per client buy, and what are its limits?** Clients are served concurrently instead of waiting in the accept queue. Each
connection costs a thread and a stack; for hundreds of clients that is fine, for tens of thousands you would switch to `epoll`.

**Why can single- and multi-threaded look equal on your LAN?** If one client already saturates the link (or the server disk), serving
clients one after another loses nothing. The multi-threaded server only wins when a single client leaves server capacity idle.

**Where exactly is the failure injected, and why a reset?** After each file header and before each 64 KB chunk, using `rand_r` with an
independent seed per connection. `SO_LINGER` with timeout 0 makes `close()` send a RST, which is what a crashed peer looks like; a
graceful close would let queued data through and be distinguishable from a real failure.

**Why does Case 1 collapse as failures increase?** Each attempt must survive all N checks: (1−p)^N. Expected attempts grow as e^(pN), and
each failed attempt wastes about half a transfer on average.

**What is stored in a checkpoint, and who validates it?** Topic, current file, byte offset, files completed (client, in
`.checkpoint.tmp`). On reconnect the client sends `CMD_RESUME(topic, file, offset)`; the server checks that the topic and file exist and that
`offset ≤ file size`, and answers `ERR_BAD_OFFSET` otherwise, in which case the client restarts that file.

**What if the checkpoint is wrong but looks valid?** We do not detect it (no checksum or modification time). That is a stated limitation;
a CRC of the downloaded prefix or the file's mtime would fix it. On the client we always clamp the offset to the real local file length.

**Why is the server-sent byte count larger than the useful bytes in Case 2?** The server hands data to the kernel faster than the network
drains it; when it resets, whatever sits in the send queue is discarded and later re-sent. The client never receives a byte twice, so its
redundant count is 0 — the waste is visible only on the server side.

**What does Enhanced parallelise, and how is a file reassembled?** Whole files, not parts of files, so there is no reassembly. K
connections take files from a shared queue; each keeps several requests in flight so it never idles for a round trip.

**Why pipelining?** With one request per file the connection idles for a round trip between files; on thousands of small images that
made Enhanced slower than plain Case 2 whenever there was latency. Pipelining fixed it without any protocol change.

**What happens if one of the K connections fails?** Only that connection reconnects; it resumes its current file with `CMD_RESUME` and
re-requests the not-yet-started files in its window. The others keep going.

**Why `sendfile`, and does it affect fault injection?** The kernel copies file pages straight to the socket (no copy through user space).
We call it in 64 KB steps so the per-chunk fault check is the same as in the buffered path; `buffered` is available to measure the difference.

**Blocking or non-blocking sockets?** Blocking, one thread per connection. Non-blocking I/O with `epoll` would reduce thread count but is
not needed at lab scale and complicates the code.

**What if two clients ask for the same topic?** The topic table is built once at start-up and only read afterwards, and every thread opens
its own file descriptors, so no locking is needed on the data path.

**How do you stop a client reading files outside the topic folder?** Topic and file names are validated (no `/`, no `..`), lookups go through
the pre-scanned table, and payload sizes are bounded.

**How did you test it?** `tools/verify.py`: 17 checks (byte-identical downloads for all cases with faults, fault counters agree between
server and clients, resume after `kill -9`, graceful shutdown, protocol refusals), also under AddressSanitizer, UBSan and ThreadSanitizer.

**What would you improve?** Checksums on resume, adaptive K and window size, exact wire-byte accounting with `TCP_INFO`, `epoll` for a
very large number of clients.

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `make: No targets specified and no makefile found` | you are in the wrong folder; `cd` to the project root (or a `PartX/` folder) |
| client: `cannot connect` / hangs | server not running, wrong IP/port, firewall (`sudo ufw allow 5000/tcp`), different network |
| `Address already in use` on the server | previous server still running (`pkill server`) or pick another port |
| Case 1 never finishes | failure probability too high for the topic size: use ≤ 2e-4 on big topics |
| `tools/lab_check.sh` says "not Linux" | you are on a Mac: use Docker (`tools/rig.sh`) |
| benchmark: `Permission denied (publickey)` | set up ssh keys, or use `--manual-server` |
| numbers are noisy / first run slow | page cache: warm up the dataset, use `--repeat 3`, drop run 1 |
| `rig.sh shape` fails | netem not in the Docker VM: use `tools/netem_proxy.py` |

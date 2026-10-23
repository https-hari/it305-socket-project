# IT305 Socket Programming Project — Groups 095 & 120

Implementation of the **Fault-Tolerant Topic-Based File Distribution Service** described in `Design_Document_Group_095_120.pdf` (the design PDF is not duplicated in this source bundle; submit it separately only if your instructor asks for it with the archive).

## 1. Environment

This is POSIX C code and is intended for Ubuntu/Linux or Ubuntu under WSL. It uses TCP sockets, `pthread`, `sendfile`, `dirent`, and POSIX file operations. Native Windows/MSVC is not the target environment.

Required tools:

```bash
sudo apt update
sudo apt install build-essential make python3 python3-pip
```

The benchmark graph generator uses Matplotlib:

```bash
python3 -m pip install matplotlib
```

## 2. Directory structure

```text
FinalSubmission_Group_095_120/
├── README.md
├── FinalReport_Group_095_120.pdf       # draft/template until real results are inserted
├── FinalReport_Group_095_120.md        # editable report source
├── sync_common.sh                      # copies common/ into every Part*/ (see below)
├── common/                             # single source of truth for the shared code
│   ├── protocol.h  io_helpers.[ch]  server_core.[ch]  client_core.[ch]
├── Part1/                              # single-threaded / multi-threaded switch
│   ├── server.c  client.c  Makefile  common/     # <- self-contained copy of ../common
├── Part2_Case1/                        # fault injection; restart the topic from byte zero
├── Part2_Case2/                        # resumable full-topic transfer with client checkpoint
├── Part2_Case2_Enhanced/               # manifest, K parallel file workers, resume, sendfile, socket tuning
├── docs/
│   └── LAB_AND_VIVA_GUIDE.md           # system overview, rehearsal, lab runbook, viva demo script and Q&A
├── tools/
│   ├── verify.py                       # 17 end-to-end checks: correctness, faults, crash-resume, shutdown
│   ├── demo.py                         # narrated one-machine demo of all four ideas (viva backup)
│   ├── rig.sh  docker-ssh              # three-container rehearsal of the three-PC setup (Docker)
│   ├── netem_proxy.py                  # software delay / bandwidth-cap proxy (no root needed)
│   └── lab_check.sh                    # pre-flight check to run on every lab PC
└── Results/
    ├── raw_metrics.csv                 # header only until the lab experiments have been run
    ├── benchmark.py                    # experiment runner (local, ssh or manual) + graph generator
    └── README.md
```

**Why `common/` is copied into every case folder.** The handout requires each `Part*/` folder to be self-contained
(own server code, client code and Makefile). The Makefiles use `./common` when it exists and fall back to `../common`.
Edit only the top-level `common/`, then run `./sync_common.sh` (and `./sync_common.sh --check` before submitting to
confirm no copy is stale).

Each `Part*/` directory contains its own `server.c`, `client.c`, and `Makefile`. Those small entry points select the case-specific behavior in `common/`; the shared wire protocol, registry, and I/O code are compiled into each executable. Running `make` in a case directory creates exactly two executable files there: `server` and `client`.

## 3. Build

From the project root:

```bash
make -C Part1 clean all
make -C Part2_Case1 clean all
make -C Part2_Case2 clean all
make -C Part2_Case2_Enhanced clean all
```

Or build all four in one loop:

```bash
for d in Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced; do
    make -C "$d" clean all || exit 1
done
```

## 4. Dataset layout

Pass the parent directory containing topic subdirectories. Do not hard-code dataset paths in the programs. Topic names are directory names. Lookup tries an exact match first and then falls back to a case-insensitive match (so `Cane` finds `cane`).

```text
/path/to/topics_root/
├── Cane/
│   ├── image001.jpg
│   └── image002.jpg
├── Cavallo/
├── Elefante/
└── ...
```

The server scans the root at startup, resolves the topic directories to absolute paths, scans regular files, and sorts file names so initial transfers and resumptions use deterministic ordering.

## 5. Required command-line interface

### Part I: single-threaded and multi-threaded server

On the server machine:

```bash
cd Part1
./server <port> <topics_root_dir>
```

The required two-argument form defaults to **multi-threaded** operation. For the Part I single-vs-multi performance experiment only, use the optional trailing argument:

```bash
./server <port> <topics_root_dir> single
./server <port> <topics_root_dir> multi
```

On a client machine (exact required interface):

```bash
cd Part1
./client <server_ip> <server_port> <topic> <output_dir>
```

The downloaded files are placed in `<output_dir>/<topic>/`.

### Part II, Case 1: no session management

Server:

```bash
cd Part2_Case1
./server <port> <topics_root_dir> <failure_probability>
```

Client:

```bash
./client <server_ip> <server_port> <topic> <output_dir>
```

The client retries after a dropped connection and deletes the partial topic directory before requesting the whole topic again. This intentionally demonstrates restart-from-zero overhead. The Case 1 server refuses `CMD_RESUME` because it keeps no session state.

### Part II, Case 2: session management / resume

Server:

```bash
cd Part2_Case2
./server <port> <topics_root_dir> <failure_probability>
```

Client:

```bash
./client <server_ip> <server_port> <topic> <output_dir>
```

The client stores its checkpoint (topic, current file, byte offset, completed-file count) at `<output_dir>/<topic>/.checkpoint.tmp` as a fixed 512-byte record that is rewritten in place with one `pwrite()` after every chunk, and sends `CMD_RESUME` on reconnect. When a checkpoint is loaded, the stored offset is clamped to the length of the local file, so a stale record can never skip data. The server validates the topic, file, and offset before resuming. The server does not retain a durable session table between connections; this follows the submitted design document's stateless per-request validation decision.

### Part II, Case 2 — Enhanced

Server:

```bash
cd Part2_Case2_Enhanced
./server <port> <topics_root_dir> <failure_probability> [sendfile|buffered]
```

The optional fourth argument selects the data path (default `sendfile`); `buffered` uses `pread()`+`send()` and exists so the report can quantify the benefit of zero-copy transmission.

Client, using the required four arguments (default K = 4):

```bash
./client <server_ip> <server_port> <topic> <output_dir>
```

Optional K parallel file workers:

```bash
./client <server_ip> <server_port> <topic> <output_dir> 8
```

The enhanced client fetches the manifest, assigns whole files through a mutex-protected queue, records each completed file in an append-only journal (`.checkpoint.tmp`, one `D<TAB>name` line per file; progress inside an unfinished file is recovered from its length on disk), and uses up to K persistent worker connections (plus one short-lived connection that fetches the manifest). Each worker **pipelines** its requests: up to 8 file requests (`IT305_ENH_WINDOW`) are in flight on its connection, so it never waits a round trip between files; after a drop it resumes its current file with `CMD_RESUME` and re-requests the rest of its window. A worker reconnects only if its connection fails. The enhanced server uses 256-KB socket buffers and bounded `sendfile()` payload sends while preserving the 8-byte header before every `FILE_DATA` payload.

### Fault-injection mechanism

The server's `<failure_probability>` argument is the probability *p* that a connection is cut at each **fault check**. A check
happens in the worker thread that serves the client: once right after a file's `FILE_HEADER` has been sent (before any data of
that file) and again before every 64 KB `FILE_DATA` chunk. At each check the thread draws a uniform random number with `rand_r`
(from its own per-connection seed) and, if it is below *p*, it sets `SO_LINGER` to `{on, 0}` and shuts the socket down. The worker
then closes it, so the connection is aborted abruptly (the kernel discards unsent data) instead of finishing gracefully; the client
sees an early end of stream or a reset in the middle of a transfer, as after a crashed link. The decision is made only on the server.
The client just notices that the connection broke and recovers according to its case: Case 1 restarts the topic from byte 0, while
Case 2 and Enhanced send `CMD_RESUME` with the file and byte offset they had reached. With `p = 0` the check does nothing, which is
how Part I and the no-fault baselines run.

Use a failure probability between `0.0` and `1.0`. The probability applies **per check**: once when a file starts and once before every 64 KB chunk. A transfer with N checks finishes without any fault with probability (1-p)^N, so on a topic with thousands of files Case 1 (which restarts from zero) practically never completes above p of about 2e-4; use values such as `0`, `1e-5`, `1e-4`, `2e-4`, or a smaller topic. A value of `1.0` drops every transfer and is not expected to complete.

### Behaviour worth knowing

- **Connection lifetime.** Whole-topic replies (`CMD_GET`, `CMD_RESUME` in Part I / Case 1 / Case 2) end with `END_OF_TOPIC` and the server closes the connection. Only the Enhanced per-file protocol keeps a connection open for the next request.
- **Server-side timing.** Each `SERVER_METRIC` line carries the connection's active send window (`send_sec`, `start_unix_ns`, `end_unix_ns`: first request to last finished reply, from `clock_gettime`); `benchmark.py` turns them into `server_window_sec` and `server_aggregate_throughput_Bps`.
- **Fault injection RNG.** Every connection gets an independent seed (clock nanoseconds, pid and a connection counter mixed with splitmix64) for `rand_r`, so faults are independent across reconnects.
- **Reconnect delay.** Clients reconnect immediately by default. Set `IT305_RECONNECT_DELAY_MS=<ms>` to add a back-off; if you do, state it in the report because it directly adds to completion time.
- **Graceful shutdown.** `SIGINT`/`SIGTERM` stop accepting new connections; in-flight transfers are allowed to finish (up to about 10 s) before the server exits.
- **Checkpoint durability.** Client checkpoints and downloaded data are **not** `fsync()`ed per chunk. They survive a client *process* crash (the page cache outlives the process), which is what the tests exercise, but not an operating-system crash or power loss. Per-chunk `fsync` was removed because its cost depends heavily on the disk and would otherwise dominate every throughput measurement.
- **Bytes counted.** The client reports `received_payload_bytes` (what crossed the network and was read) and `useful_payload_bytes` (what ended up in the output files); redundant = received - useful. The server's `SERVER_METRIC data_bytes` counts bytes handed to the kernel; after a TCP reset the kernel discards what is still queued, so server-sent bytes can exceed client-received bytes. That gap is wasted bandwidth and is plotted as "server-sent excess" in the overhead graphs.

## 6. Protocol summary

All frames have an 8-byte big-endian header: message type (1 byte), flags (1 byte), status (2 bytes), payload length (4 bytes). The data payload is at most 64 KiB per `FILE_DATA` message. The opcodes and payload layout are centralized in `common/protocol.h` and implemented by the framing helpers in `common/io_helpers.c`.

- `CMD_GET` / `FILE_HEADER` / `FILE_DATA` / `END_OF_TOPIC`: full-topic transfer.
- `CMD_RESUME`: topic, file name, and byte offset; server validates offset against the current file size.
- `CMD_MANIFEST` / `MANIFEST_DATA` / `CMD_GET_FILE` / `FILE_COMPLETE`: enhanced per-file parallel transfer.
- `ERR_NOT_FOUND`, `ERR_BAD_OFFSET`, `ERR_PROTOCOL`: application errors.

## 7. Testing before the lab experiment

Build everything, then run the automated checks against any topics folder (the Kaggle `raw-img` folder works):

```bash
python3 tools/verify.py /path/to/raw-img
```

It verifies, for all four cases and several concurrent clients, that downloads are byte-identical with and without fault
injection; that topic names match case-insensitively and unknown topics fail cleanly; that Part I / Case 1 refuse
`CMD_RESUME`; that a client killed with `SIGKILL` mid-transfer resumes (Case 2 and Enhanced) and receives fewer bytes
on the second run; and that `SIGINT` drains in-flight transfers. Expect `17/17 checks passed`.

Manual example:

```bash
cd Part2_Case2
./server 5000 /path/to/raw-img 0.0001
# in another terminal
./client 127.0.0.1 5000 cane /tmp/it305_output
```

Stop the server with Ctrl+C. For a remote client, replace `127.0.0.1` with the server machine's reachable IP and allow the chosen TCP port through the host firewall.

## 8. Performance measurements and final report

The assignment requires one server computer and clients on two other computers. `Results/benchmark.py` can drive
the whole experiment from one machine over ssh (passwordless ssh, project built with `make` in the same directory on
all three machines, dataset on the server machine):

```bash
python3 Results/benchmark.py --topic cane --client-counts 1,2,4,8,16 \
    --failure-probabilities 0,0.00001,0.0001,0.0002 --repeat 3 \
    --server-host <server_ip> --server-ssh <user>@<server_ip> --peer-ssh <user>@<peer_ip> \
    --remote-dir /path/to/FinalSubmission_Group_095_120 --remote-topics /path/to/raw-img --no-build
```

Half of the clients (rounded down) run on `--peer-ssh`, the rest on the machine you start the script on. If ssh between
the lab machines is not allowed, add `--manual-server` (the script prints the exact server command for every scenario and
waits for you) and start the clients from the two client machines yourselves. Aggregate throughput is the sum of the
clients' useful bytes divided by the slowest client's own elapsed time. For the sendfile ablation add
`--server-data-path buffered` in a second run and compare with the default; for the parallel-connection ablation vary
`--parallel-connections`.

Raw rows go to `Results/raw_metrics.csv` and graphs (`Part1_throughput.png`, `Part2_Case*_throughput.png`,
`Part2_Case*_overhead.png`) are written next to it; `--graphs-only` redraws them from the CSV, `--append` keeps earlier
rows. Run `python3 Results/benchmark.py --help` for all options.

`Results/raw_metrics.csv` initially contains only its header. No performance results have been fabricated. **The included `FinalReport_Group_095_120.pdf` is a draft template until the actual experiment tables/graphs and each member's real one-line contribution are inserted. Do not submit that draft unchanged.** The editable `FinalReport_Group_095_120.md` has the same sections to update.

The assignment requires an archive named exactly:

```text
FinalSubmission_Group_095_120.zip
```

Create it from the project directory's parent, so the archive contains one top-level `FinalSubmission_Group_095_120/` folder. Before archiving: run `./sync_common.sh --check`, build and run `tools/verify.py`, and remove generated executables with `make -C <case> clean` (the Makefiles recreate `server` and `client`).

## 9. Running in Docker (macOS / Windows / any machine without Linux)

The code needs Linux, so on a Mac use the provided `Dockerfile` (Ubuntu 24.04, gcc 13, Python 3). The project folder is
mounted into the container, so you edit on your Mac and build/run in Linux.

**Dataset location (kept outside the project so it never ends up in the submission zip):**
`~/Desktop/it305_data/raw-img/<topic folders>` (download: https://www.kaggle.com/datasets/alessiocorrado99/animals10).

```bash
cd ~/Desktop/FinalSubmission_Group_095_120
docker build -t it305 .                                    # once
docker run --rm -it -v "$PWD":/app -v "$HOME/Desktop/it305_data/raw-img":/data:ro it305
#   inside the container:
make                         # builds Part1, Part2_Case1, Part2_Case2, Part2_Case2_Enhanced
make test                    # = python3 tools/verify.py /data   (expect: 17/17 checks passed)
```

Separate "machines" on one Mac (a user-defined network lets containers reach each other by name):

```bash
docker network create it305net
# terminal 1 - server
docker run --rm -it --name server --network it305net -v "$PWD":/app -v "$HOME/Desktop/it305_data/raw-img":/data:ro it305 \
    ./Part2_Case2/server 5000 /data 0.0001
# terminal 2 - client
mkdir -p out && docker run --rm -it --network it305net -v "$PWD":/app -v "$PWD/out":/out it305 \
    ./Part2_Case2/client server 5000 cane /out
```

Numbers measured inside Docker Desktop on a laptop are **not** lab results: everything shares one machine and a virtual
network. Use Docker for building, testing and understanding; take the report's measurements on the lab computers.
Before submitting run `make clean` (the Linux binaries built in Docker are useless on a Mac and must not be in the zip).

## 10. Testing alone, rehearsing, and the viva

`docs/LAB_AND_VIVA_GUIDE.md` has the system explained on one page, the design alternatives, what results to expect, the lab-day
runbook for three computers, a timed viva demo script and likely questions with answers. Tools for working alone:

| Tool | Purpose |
|---|---|
| `tools/rig.sh up / test / shape / bench / down` | three Docker containers stand in for server + two client PCs; `bench` runs the same orchestrated experiment as the lab |
| `tools/netem_proxy.py` | delay and per-connection bandwidth cap between client and server, no root (used by `demo.py`) |
| `tools/demo.py --topics-root <dir>` | two-minute narrated demo: single vs multi, Case 1 vs 2 vs Enhanced, parallel connections, crash-resume |
| `tools/lab_check.sh [server_ip port]` | run on every lab PC first: tools, build, loopback self-test, IP address, reachability of the server |

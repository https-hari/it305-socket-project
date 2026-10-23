# Compliance checklist: handout and design document → evidence

Status: **DONE** = implemented and tested · **LAB** = needs the three-computer measurements · **NOTE** = deliberate choice, explain it.
Automated checks: `tools/verify.py` (17), `tools/edge_tests.py` (36), `tools/check_submission.py` (submission gate).

## A. Standard CLI interface (auto-grader rules)

| Requirement | Status | Evidence |
|---|---|---|
| `./server <port> <topics_root_dir>` for Part 1 | DONE | `Part1/server.c`; optional 3rd argument `single\|multi`, default `multi` |
| `./server <port> <topics_root_dir> <failure probability>` for every Part 2 case | DONE | `Part2_*/server.c`; value validated to 0..1, usage error exit 2 |
| `./client <server_ip> <server_port> <topic> <output_dir>`; writes `output_dir/topic/` | DONE | all four `client.c`; output folders created as needed |
| No hard-coded path, port or IP; no interactive prompts | DONE | audited by grep (nothing found); everything comes from arguments |
| Extra arguments only as optional trailing ones with defaults | DONE | Enhanced client `[K]` (default 4), Enhanced server `[sendfile\|buffered]` |
| `make` in each `Part*/` produces exactly `server` and `client` there | DONE | checked from a fresh archive extract by `tools/check_submission.py` |
| Executables named exactly `server` / `client` | DONE | Makefiles |

## B. Part I

| Requirement | Status | Evidence |
|---|---|---|
| Server keeps topic → directory map (hash table) | DONE | `TopicRegistry` in `common/server_core.c` |
| Client `GET <topic>`; server sends all files of the topic | DONE | `CMD_GET`; hidden files included, sub-folders and symlinks skipped (NOTE) |
| Single-threaded server (sequential) and multi-threaded server | DONE | `Part1/server [single\|multi]` |
| Client creates the topic directory and stores the files | DONE | byte-identical in `verify.py` |
| Measure time with `clock_gettime`, bytes sent, aggregate rate | DONE | server `SERVER_METRIC` (send window, bytes), client `RESULT` (elapsed, bytes) |
| Experiments on 3 computers: server on one, clients on the other two | LAB | `benchmark.py --server-ssh … --client-ssh …` / `--manual-server` |
| Table/graph: aggregate rate vs number of clients, single vs multi | LAB | `Results/Part1_throughput.png`, report §8.1 |
| Analysis; explain design decision for the multi-threaded server | LAB / DONE | design decision in report §4 and `docs/LAB_AND_VIVA_GUIDE.md` §2; analysis text TBD |

## C. Part II

| Requirement | Status | Evidence |
|---|---|---|
| Fault injection controlled by `<failure probability>`; document when and how | DONE | README "Fault-injection mechanism", report §3: check after each file header and before each 64 KB chunk; RST via `SO_LINGER 0` |
| All cases on the multi-threaded server, one thread per client | DONE | `Part2_*/server.c` always multi-threaded |
| Case 1: no state; restart from the beginning | DONE | `Part2_Case1`; client removes partial data |
| Case 2: checkpoint (topic, file, byte offset), updated periodically; client reports it on reconnect; server validates and resumes | DONE | checkpoint after every chunk; `CMD_RESUME`; server checks topic, file and offset (`ERR_BAD_OFFSET`) |
| "Client and server maintain some state" | NOTE | state is on the client; the server is stateless and validates each offset. The handout allows "the server, the client, or both"; this is the design submitted earlier |
| No already-recorded data is sent again | DONE | client-side redundant bytes = 0 in Case 2 (measured, not assumed) |
| Compare with/without session management over fault rates: total bytes, useful, redundant, completion time, throughput vs overhead, vs Part 1 | LAB | CSV columns + `Results/Part2_*_throughput/overhead.png`; report §8.2, §8.3 |
| Enhanced: blocking or non-blocking sockets (choose) | DONE | blocking sockets, thread per connection; justification in report |
| Enhanced: parallel TCP connections; document count, work split, reassembly, failure recovery | DONE | K connections (default 4), whole files from a shared queue, pipelined requests, per-connection resume; report §4.3 |
| Report the enhancements and measure the improvement with justification | LAB | `--label`, `--parallel-connections`, `--server-data-path` ablation runs; report §8.4 |

## D. Submission

| Requirement | Status | Evidence |
|---|---|---|
| Design document `Design_Document_Group_XXX_YYY_.pdf` | DONE | submitted earlier (95 and 120) |
| Archive `FinalSubmission_Group_<XXX>_<YYY>.zip` or `.tar.gz`, no loose files | DONE | `tools/check_submission.py` writes `FinalSubmission_Group_095_120.zip` |
| `FinalReport_Group_<XXX>_<YYY>.pdf`: one consolidated report | LAB | template complete; tables/analysis after the experiments (`tools/build_report.sh`) |
| `Part1/`, `Part2_Case1/`, `Part2_Case2/`, `Part2_Case2_Enhanced/` each with server, client, Makefile | DONE | gate check |
| `Results/` with raw `.csv` and graphs per case (`Partname_CaseName_throughput.png`, `_overhead.png`) | LAB | produced by `benchmark.py` with exactly those names |
| `README.md`: build/run for every case and the fault-injection mechanism | DONE | gate check |
| Report: separate Part I / Case 1 / Case 2 / Enhanced; cover page with names, roll numbers, one-line contributions | LAB | contributions are TBD placeholders for the authors; the gate flags them |
| Report: design section with implementation detail and deviations from the submitted design | DONE | report §4 to §7 (deviations listed in §6) |
| Report: performance section with throughput and overhead tables/graphs and analysis | LAB | §8 |
| Academic integrity: own work, attribute outside code | NOTE | no outside code was copied; every member must understand all of it (see the viva guide) |

## E. Promises of the submitted design document

| Design-document item | Status | Evidence / note |
|---|---|---|
| Hash-backed topic registry with `pthread_rwlock`, sorted file lists | DONE | `TopicRegistry` |
| Fixed 8-byte binary header, opcode table, reserved `ACK` | DONE | `common/protocol.h` |
| `CMD_RESUME` + server-side validation (existence, bounds, offset == size) | DONE | `handle_request` |
| Enhanced: manifest, per-file request, `FILE_COMPLETE`, K connections (default 4), `sendfile`, 256 KB buffers | DONE | |
| Fault check at start of each file and before each chunk; `SO_LINGER` RST | DONE | `stream_file_framed`, `fault_now` |
| Checkpoint journal `.checkpoint.tmp` | DONE | Case 2: fixed record, rewritten in place; Enhanced: append-only completed-file journal (NOTE: differs from the design's per-chunk array; listed as a deviation) |
| `fsync()` of every checkpoint | NOTE | removed on purpose: throughput would measure the disk; survives a process crash, not an OS crash (report §5) |
| Thread-safe structured logging: ms timestamps, thread id, client IP, action, offsets; stdout and append-only file | DONE | `log_event`; `IT305_LOG_FILE`, `IT305_LOG_LEVEL=debug` |
| Graceful SIGINT/SIGTERM shutdown | DONE | drains in-flight transfers |
| Dynamic directory reload on SIGHUP | DONE | `kill -HUP <server pid>` re-scans the topics folder; tested under load |
| Directory sandboxing (no `..`, no absolute paths) | DONE | name validation on both sides; tested with hostile input |
| Single binary with optional mode argument for Part I | DONE | |
| Extensibility to new topics | DONE | any new sub-folder is a topic (SIGHUP, no restart) |

## F. Extras we added on our own initiative

Pipelined requests in the Enhanced client (measured to matter), server-side send-window timing, `benchmark.py` three-computer
orchestration with incremental CSV writes and ablation labels, report-table generator, 36-check robustness suite, ThreadSanitizer /
AddressSanitizer / UBSan runs, Docker rehearsal rig, software network emulator, narrated demo, lab pre-flight check, submission gate.

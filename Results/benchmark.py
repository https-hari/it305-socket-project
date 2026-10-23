#!/usr/bin/env python3
"""Run multi-client performance experiments and generate graphs.

Three ways to run it
  local        everything on this machine (development / smoke test)
  ssh          the lab setup required by the assignment, orchestrated from ONE machine:
                 --server-ssh user@server   starts ./server on the server computer
                 --peer-ssh   user@peer     runs half of the clients on the second client computer
               this machine runs the other half of the clients. Needs passwordless ssh, the project
               built (`make`) in --remote-dir on the server and peer machines, and the topics folder
               at --remote-topics on the server machine.
  manual       --manual-server: prints the exact server command for every scenario and waits for you
               (use it if ssh between lab machines is not allowed).

Aggregate throughput = sum of useful bytes of all clients / the slowest client's own elapsed time, so
ssh start-up latency of the remote clients does not distort it.
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = Path(__file__).resolve().parent
CSV_PATH = RESULTS_DIR / "raw_metrics.csv"
FIELDS = [
    "scenario", "case", "mode", "client_count", "failure_probability", "repeat",
    "elapsed_wall_sec", "client_elapsed_sec_sum", "server_data_bytes_sent",
    "received_payload_bytes", "useful_payload_bytes", "redundant_payload_bytes",
    "retries", "source_bytes_per_client", "aggregate_throughput_Bps", "success",
    "server_log", "client_logs", "client_elapsed_sec_max", "client_nodes",
    "server_window_sec", "server_aggregate_throughput_Bps",
    "parallel_connections", "server_data_path", "label",
]


def parse_key_values(result_line: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for field in result_line.strip().split(","):
        if "=" not in field:
            continue
        key, value = field.split("=", 1)
        values[key] = value
    return values


def read_client_metrics(path: Path) -> dict[str, Any] | None:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        if line.startswith("RESULT,"):
            try:
                raw = parse_key_values(line)
                return {
                    "elapsed_sec": float(raw["elapsed_sec"]),
                    "received_payload_bytes": int(raw["received_payload_bytes"]),
                    "useful_payload_bytes": int(raw["useful_payload_bytes"]),
                    "redundant_payload_bytes": int(raw["redundant_payload_bytes"]),
                    "retries": int(raw["retries"]),
                    "throughput_Bps": float(raw["throughput_Bps"]),
                }
            except (KeyError, TypeError, ValueError):
                return None
    return None


def source_topic_bytes(topics_root: Path, topic: str) -> int:
    topic_dir = topics_root / topic
    if not topic_dir.is_dir():
        raise FileNotFoundError(f"Topic directory does not exist: {topic_dir}")
    total = 0
    for p in topic_dir.iterdir():
        try:
            if p.is_file() and not p.is_symlink():
                total += p.stat().st_size
        except OSError:
            continue
    return total


def server_data_bytes(path: Path) -> int:
    return server_metrics(path)[0]


def server_metrics(path: Path) -> tuple[int, float]:
    """(total data bytes the server wrote, seconds from the first request to the last finished reply).

    The window is the span of the per-connection [first request, last reply] intervals the server logged with
    clock_gettime, i.e. the handout's "time required to send data", so bytes/window is the server-side aggregate
    transfer rate."""
    total = 0
    first, last = None, None
    pattern = re.compile(r"SERVER_METRIC,case=\d+,data_bytes=(\d+),[^\n]*?start_unix_ns=(\d+),end_unix_ns=(\d+)")
    plain = re.compile(r"SERVER_METRIC,case=\d+,data_bytes=(\d+),")
    try:
        for line in path.read_text(errors="replace").splitlines():
            m = pattern.search(line)
            if m:
                nbytes, t0, t1 = int(m.group(1)), int(m.group(2)), int(m.group(3))
                total += nbytes
                if nbytes > 0 and t0 and t1 >= t0:
                    first = t0 if first is None else min(first, t0)
                    last = t1 if last is None else max(last, t1)
                continue
            m = plain.search(line)
            if m:
                total += int(m.group(1))
    except OSError:
        pass
    window = (last - first) / 1e9 if first is not None and last is not None and last > first else 0.0
    return total, window


def wait_until_ready(proc: subprocess.Popen[str], log_path: Path, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            text = log_path.read_text(errors="replace")
            if "[LISTENING]" in text:
                return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def available_port(start: int, attempts: int = 100) -> int:
    for port in range(start, min(start + attempts, 65536)):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"Could not find a free port in {start}..{min(start + attempts - 1, 65535)}")


def run_build(case_dir: Path) -> None:
    subprocess.run(["make", "clean", "all"], cwd=case_dir, check=True)


def ssh_argv(target: str, remote_cmd: str, tty: bool = False) -> list[str]:
    """Build an ssh command line. IT305_SSH overrides the ssh binary (used by the self-test)."""
    argv = shlex.split(os.environ.get("IT305_SSH", "ssh")) + ["-o", "BatchMode=yes",
                                                                "-o", "StrictHostKeyChecking=accept-new"]
    if tty:
        argv.append("-tt")      # a pty makes the remote server die when this ssh process is terminated
    return argv + [target, remote_cmd]


def dir_bytes(path: Path) -> int:
    total = 0
    try:
        for p in path.iterdir():
            if p.is_file() and not p.name.startswith("."):
                total += p.stat().st_size
    except OSError:
        pass
    return total


def warm_cache(a, topics_root: Path | None, topic: str) -> None:
    """Read the topic once on the server machine so that the first measured scenario does not pay for cold disk reads."""
    if a.manual_server:
        print("[warmup] skipped (manual server): on the server PC run  find <topics_root>/<topic> -type f -exec cat {} + > /dev/null")
        return
    if a.server_ssh:
        remote = f"{shlex.quote(a.remote_topics or str(topics_root))}/{shlex.quote(topic)}"
        subprocess.run(ssh_argv(a.server_ssh, f"find {remote} -type f -exec cat {{}} + > /dev/null"),
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elif topics_root is not None and (topics_root / topic).is_dir():
        for f in (topics_root / topic).iterdir():
            if f.is_file():
                with f.open("rb") as fh:
                    while fh.read(1 << 20):
                        pass
    print(f"[warmup] topic '{topic}' read once into the server's page cache")


def run_one_scenario(case_name: str, mode: str, client_count: int, failure_probability: float,
                     repeat_idx: int, port_start: int, topics_root: Path | None, topic: str, a) -> dict[str, Any]:
    case_dir = PROJECT_ROOT / case_name
    run_name = f"{case_name}_{mode}_clients{client_count}_p{failure_probability:g}_r{repeat_idx}"
    if a.label:
        run_name += "_" + "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in a.label)
    run_dir = RESULTS_DIR / "benchmark_runs" / run_name
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    server_log = run_dir / "server.log"
    client_logs: list[Path] = []

    external = bool(a.server_ssh) or a.manual_server or a.server_host not in ("127.0.0.1", "localhost")
    if not a.no_build:
        run_build(case_dir)
    port = port_start if external else available_port(port_start)
    topics_arg = a.remote_topics or str(topics_root)
    if case_name == "Part1":
        server_args = [str(port), topics_arg, mode]
    else:
        server_args = [str(port), topics_arg, f"{failure_probability:.8f}"]
        if case_name == "Part2_Case2_Enhanced" and a.server_data_path:
            server_args.append(a.server_data_path)
    remote_case = f"{a.remote_dir.rstrip('/')}/{case_name}" if a.remote_dir else ""
    # clients are spread round-robin over the client machines: this machine (unless --no-local-clients) and
    # every --client-ssh / --peer-ssh node
    node_list = ([None] if not a.no_local_clients else []) + list(a.client_ssh_list)
    if not node_list:
        raise RuntimeError("no machine to run clients on: pass --client-ssh or drop --no-local-clients")
    assignment = [node_list[i % len(node_list)] for i in range(client_count)]
    local_n = sum(1 for n in assignment if n is None)
    remote_nodes = [n for n in node_list if n is not None]
    extra = [str(a.parallel_connections)] if case_name == "Part2_Case2_Enhanced" else []

    server: subprocess.Popen[str] | None = None
    server_out = None
    succeeded = True
    try:
        if a.manual_server:
            print(f"\n>>> On the SERVER machine run:\n    cd {a.remote_dir or case_dir}/{case_name if a.remote_dir else ''} && "
                  f"./server {' '.join(shlex.quote(x) for x in server_args)}")
            input(">>> Press Enter once it prints [LISTENING] ... ")
        elif a.server_ssh:
            cmd = ssh_argv(a.server_ssh, f"cd {shlex.quote(remote_case)} && echo $$ > /tmp/it305_server.pid && exec ./server "
                           + " ".join(shlex.quote(x) for x in server_args), tty=True)
            server_out = server_log.open("w", encoding="utf-8")
            server = subprocess.Popen(cmd, stdout=server_out, stderr=subprocess.STDOUT, text=True)
        else:
            server_out = server_log.open("w", encoding="utf-8")
            server = subprocess.Popen([str(case_dir / "server")] + server_args, cwd=case_dir,
                                      stdout=server_out, stderr=subprocess.STDOUT, text=True)
        if server is not None and not wait_until_ready(server, server_log):
            raise RuntimeError(f"Server failed to start. Log: {server_log}")

        procs: list[tuple[subprocess.Popen[str], Any]] = []
        out_dirs: list[Path] = []
        for idx in range(client_count):
            log_path = run_dir / f"client_{idx + 1}.log"
            client_logs.append(log_path)
            handle = log_path.open("w", encoding="utf-8")
            target = assignment[idx]
            if target is None:
                out_dir = run_dir / f"client_{idx + 1}_output"
                out_dirs.append(out_dir)
                cmd = [str(case_dir / "client"), a.server_host, str(port), topic, str(out_dir)] + extra
                proc = subprocess.Popen(cmd, cwd=case_dir, stdout=handle, stderr=subprocess.STDOUT, text=True)
            else:
                rdir = f"/tmp/it305_bench/{run_name}/c{idx + 1}"
                rcmd = (f"cd {shlex.quote(remote_case)} && ./client {shlex.quote(a.server_host)} {port} "
                        f"{shlex.quote(topic)} {shlex.quote(rdir)} " + " ".join(extra))
                proc = subprocess.Popen(ssh_argv(target, rcmd), stdout=handle, stderr=subprocess.STDOUT, text=True)
            procs.append((proc, handle))
        start_time = time.monotonic()
        for proc, handle in procs:
            try:
                proc.wait(timeout=a.timeout_seconds)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait(); succeeded = False
            finally:
                handle.close()
            if proc.returncode != 0:
                succeeded = False
        wall_elapsed = time.monotonic() - start_time
        time.sleep(0.3)                      # let worker threads flush their final metric lines
        if a.manual_server:
            input(">>> Stop the server (Ctrl-C) so it flushes, then press Enter ... ")
        # integrity check: every local client must hold exactly the source bytes
        src_bytes = source_topic_bytes(topics_root, topic) if topics_root and (topics_root / topic).is_dir() else 0
        if src_bytes:
            for od in out_dirs:
                if dir_bytes(od / topic) != src_bytes:
                    succeeded = False
        if not a.keep_output:
            for od in out_dirs:
                shutil.rmtree(od, ignore_errors=True)
            for node in remote_nodes:
                subprocess.run(ssh_argv(node, f"rm -rf /tmp/it305_bench/{shlex.quote(run_name)}"),
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    finally:
        if server is not None and server.poll() is None and a.server_ssh:
            # SIGINT lets the server drain and flush its SERVER_METRIC lines; works with or without a pty
            subprocess.run(ssh_argv(a.server_ssh, "kill -INT $(cat /tmp/it305_server.pid) 2>/dev/null; true"),
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                server.wait(timeout=12)
            except subprocess.TimeoutExpired:
                pass
        if server is not None and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill(); server.wait()
        if server_out is not None:
            server_out.close()

    metrics = [read_client_metrics(p) for p in client_logs]
    valid_metrics = [m for m in metrics if m is not None]
    if len(valid_metrics) != client_count:
        succeeded = False
    received = sum(m["received_payload_bytes"] for m in valid_metrics)
    useful = sum(m["useful_payload_bytes"] for m in valid_metrics)
    redundant = sum(m["redundant_payload_bytes"] for m in valid_metrics)
    retries = sum(m["retries"] for m in valid_metrics)
    elapsed_sum = sum(m["elapsed_sec"] for m in valid_metrics)
    elapsed_max = max((m["elapsed_sec"] for m in valid_metrics), default=0.0)
    server_bytes, server_window = server_metrics(server_log) if server_log.exists() else (0, 0.0)
    unique_source_bytes = source_topic_bytes(topics_root, topic) if topics_root and (topics_root / topic).is_dir() else 0
    window = elapsed_max if elapsed_max > 0 else wall_elapsed
    agg_throughput = useful / window if window > 0 else 0.0
    nodes = "+".join("local" if n is None else n for n in node_list) + (f" server={a.server_ssh or a.server_host}" if external else "")

    row = {
        "scenario": run_name,
        "case": case_name,
        "mode": mode if case_name == "Part1" else "multi",
        "client_count": client_count,
        "failure_probability": 0.0 if case_name == "Part1" else failure_probability,
        "repeat": repeat_idx,
        "elapsed_wall_sec": f"{wall_elapsed:.9f}",
        "client_elapsed_sec_sum": f"{elapsed_sum:.9f}",
        "server_data_bytes_sent": server_bytes,
        "received_payload_bytes": received,
        "useful_payload_bytes": useful,
        "redundant_payload_bytes": redundant,
        "retries": retries,
        "source_bytes_per_client": unique_source_bytes,
        "aggregate_throughput_Bps": f"{agg_throughput:.3f}",
        "success": int(succeeded),
        "server_log": str(server_log.relative_to(PROJECT_ROOT)) if server_log.exists() else "",
        "client_logs": ";".join(str(p.relative_to(PROJECT_ROOT)) for p in client_logs),
        "client_elapsed_sec_max": f"{elapsed_max:.9f}",
        "client_nodes": nodes,
        "parallel_connections": a.parallel_connections if case_name == "Part2_Case2_Enhanced" else "",
        "server_data_path": (a.server_data_path or "sendfile") if case_name == "Part2_Case2_Enhanced" else "",
        "label": a.label or "",
        "server_window_sec": f"{server_window:.9f}",
        "server_aggregate_throughput_Bps": f"{(server_bytes / server_window) if server_window > 0 else 0.0:.3f}",
    }
    print(f"[{'OK' if succeeded else 'FAILED'}] {run_name}: agg={agg_throughput / 1e6:.2f} MB/s, "
          f"received={received}, useful={useful}, redundant={redundant}, server_sent={server_bytes}, retries={retries}")
    return row


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def aggregate(rows: list[dict[str, str]], case: str, mode: str | None, metric: str,
              x_field: str, client_count: int | None = None) -> tuple[list[float], list[float]]:
    groups: dict[float, list[float]] = defaultdict(list)
    for r in rows:
        if r.get("success") != "1" or r.get("case") != case:
            continue
        if mode is not None and r.get("mode") != mode:
            continue
        if client_count is not None and int(r["client_count"]) != client_count:
            continue
        try:
            x = float(r[x_field])
            y = float(r[metric])
        except (KeyError, TypeError, ValueError):
            continue
        groups[x].append(y)
    xs = sorted(groups)
    return xs, [mean(groups[x]) for x in xs]


def _symlog_x(ax, probabilities: list[float]) -> None:
    """Failure probabilities span orders of magnitude (0, 1e-5, 1e-4 ...): use a symmetric-log x axis."""
    positive = [p for p in probabilities if p > 0]
    if positive and max(positive) / min(positive) >= 20:
        ax.set_xscale("symlog", linthresh=min(positive))


def generate_graphs(rows: list[dict[str, str]]) -> None:
    rows = [r for r in rows if not r.get("label")]      # labelled rows are ablation runs: they must not be averaged into the main graphs
    if not rows:
        print("No metric rows available; graphs were not generated.")
        return
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("Matplotlib is missing. Install it with: python3 -m pip install matplotlib", file=sys.stderr)
        return

    # Part I: required aggregate throughput vs. concurrent client count for both server modes.
    fig, ax = plt.subplots()
    plotted = False
    for mode in ("single", "multi"):
        xs, ys = aggregate(rows, "Part1", mode, "aggregate_throughput_Bps", "client_count")
        if xs:
            ax.plot(xs, [y / 1_000_000 for y in ys], marker="o", label=mode)
            plotted = True
    if plotted:
        ax.set_xlabel("Number of concurrent clients")
        ax.set_ylabel("Aggregate useful throughput (MB/s)")
        ax.set_title("Part I: single-threaded vs multi-threaded server")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(RESULTS_DIR / "Part1_throughput.png", dpi=160)
    plt.close(fig)

    # Part II: useful aggregate throughput and retransmission overhead vs fault probability.
    for case in ("Part2_Case1", "Part2_Case2", "Part2_Case2_Enhanced"):
        relevant = [r for r in rows if r.get("success") == "1" and r.get("case") == case]
        if not relevant:
            continue
        counts = sorted({int(r["client_count"]) for r in relevant})
        fig, ax = plt.subplots()
        plotted = False
        for count in counts:
            xs, ys = aggregate(rows, case, None, "aggregate_throughput_Bps", "failure_probability", count)
            if xs:
                ax.plot(xs, [y / 1_000_000 for y in ys], marker="o", label=f"{count} client(s)")
                plotted = True
        if plotted:
            _symlog_x(ax, [float(r["failure_probability"]) for r in relevant])
            ax.set_xlabel("Failure probability")
            ax.set_ylabel("Aggregate useful throughput (MB/s)")
            ax.set_title(f"{case}: throughput vs fault-injection probability")
            ax.grid(True, alpha=0.3)
            ax.legend()
            fig.tight_layout()
            fig.savefig(RESULTS_DIR / f"{case}_throughput.png", dpi=160)
        plt.close(fig)

        fig, ax = plt.subplots()
        plotted = False
        for count in counts:
            # Compute mean overhead from aggregate byte totals for each probability.
            groups: dict[float, list[float]] = defaultdict(list)
            for r in relevant:
                if int(r["client_count"]) != count:
                    continue
                try:
                    p = float(r["failure_probability"])
                    useful = float(r["useful_payload_bytes"])
                    redundant = float(r["redundant_payload_bytes"])
                    groups[p].append((100.0 * redundant / useful) if useful > 0 else 0.0)
                except (ValueError, KeyError):
                    continue
            xs = sorted(groups)
            ys = [mean(groups[x]) for x in xs]
            if xs:
                line, = ax.plot(xs, ys, marker="o", label=f"{count} client(s): redundant bytes received")
                plotted = True
                # bytes the server pushed into the socket beyond the useful payload (includes data lost in flight at a reset)
                sg: dict[float, list[float]] = defaultdict(list)
                for r in relevant:
                    try:
                        useful = float(r["useful_payload_bytes"]); sent = float(r["server_data_bytes_sent"])
                        if int(r["client_count"]) == count and useful > 0 and sent > 0:
                            sg[float(r["failure_probability"])].append(100.0 * max(sent - useful, 0.0) / useful)
                    except (ValueError, KeyError):
                        continue
                if sg:
                    sx = sorted(sg)
                    ax.plot(sx, [mean(sg[x]) for x in sx], marker="s", linestyle="--", color=line.get_color(),
                            label=f"{count} client(s): server-sent excess")
        if plotted:
            _symlog_x(ax, [float(r["failure_probability"]) for r in relevant])
            ax.set_xlabel("Failure probability")
            ax.set_ylabel("Overhead relative to useful payload (%)")
            ax.set_title(f"{case}: retransmission overhead")
            ax.grid(True, alpha=0.3)
            ax.legend()
            fig.tight_layout()
            fig.savefig(RESULTS_DIR / f"{case}_overhead.png", dpi=160)
        plt.close(fig)
    print(f"CSV: {CSV_PATH}")
    print("Graph files written for cases with successful measurements.")


def parse_csv_list(value: str, convert):
    try:
        return [convert(x.strip()) for x in value.split(",") if x.strip()]
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from e


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topics-root", type=Path, help="parent directory containing topic subdirectories")
    parser.add_argument("--topic", help="topic directory name (case-sensitive)")
    parser.add_argument("--client-counts", default="1,2,4", help="comma-separated client counts, e.g. 1,2,4")
    parser.add_argument("--failure-probabilities", default="0,0.00001,0.0001,0.0002",
                        help="comma-separated values for Part II. The probability applies per check (file start and "
                             "every 64 KB chunk); on topics with thousands of files Case 1 practically never finishes "
                             "above ~2e-4, so keep it small or use a smaller topic")
    parser.add_argument("--repeat", type=int, default=1, help="number of repetitions per scenario")
    parser.add_argument("--port-start", type=int, default=47000)
    parser.add_argument("--parallel-connections", type=int, default=4, help="K for Case 2 Enhanced")
    parser.add_argument("--timeout-seconds", type=float, default=300.0, help="per-client time limit; a client that exceeds it is killed and the scenario marked FAILED")
    parser.add_argument("--server-host", default="127.0.0.1", help="address clients connect to (the server machine)")
    parser.add_argument("--server-ssh", help="user@host: start the server there over ssh (needs --remote-dir and --remote-topics)")
    parser.add_argument("--client-ssh", help="comma-separated user@host list: machines that run clients (any number), over ssh")
    parser.add_argument("--peer-ssh", help="user@host: one more client machine (kept for compatibility; same as adding it to --client-ssh)")
    parser.add_argument("--no-local-clients", action="store_true", help="do not run clients on this machine (use when it only orchestrates)")
    parser.add_argument("--remote-dir", help="project root on the server/peer machines (same path on both), built with make")
    parser.add_argument("--remote-topics", help="topics root directory ON THE SERVER machine (default: --topics-root)")
    parser.add_argument("--manual-server", action="store_true", help="print the server command per scenario and wait instead of using ssh")
    parser.add_argument("--no-build", action="store_true", help="do not run `make clean all` locally before each scenario")
    parser.add_argument("--keep-output", action="store_true", help="keep downloaded files (they can be large)")
    parser.add_argument("--server-data-path", choices=["sendfile", "buffered"], help="Case 2 Enhanced only: server data path for the ablation study")
    parser.add_argument("--no-warmup", action="store_true", help="skip reading the topic once before the first scenario (a cold first run distorts the first row)")
    parser.add_argument("--label", default="", help="tag stored in the CSV for an ablation/variant run (e.g. K1, buffered); labelled rows are kept out of the main graphs")
    parser.add_argument("--only", default="part1,case1,case2,enhanced", help="comma-separated subset of experiments to run: part1,case1,case2,enhanced")
    parser.add_argument("--append", action="store_true", help="append new experiment rows instead of replacing raw_metrics.csv")
    parser.add_argument("--graphs-only", action="store_true", help="generate graphs from existing raw_metrics.csv")
    args = parser.parse_args()

    existing = read_rows(CSV_PATH) if args.append or args.graphs_only else []
    if args.graphs_only:
        generate_graphs(existing)
        return 0
    if args.topic is None:
        parser.error("--topic is required unless --graphs-only is used")
    remote_server = bool(args.server_ssh) or args.manual_server
    args.client_ssh_list = [x.strip() for x in (args.client_ssh or "").split(",") if x.strip()] + ([args.peer_ssh] if args.peer_ssh else [])
    if (args.server_ssh or args.client_ssh_list) and not args.remote_dir:
        parser.error("--server-ssh/--client-ssh/--peer-ssh need --remote-dir (project root on the remote machines)")
    if args.no_local_clients and not args.client_ssh_list:
        parser.error("--no-local-clients needs at least one --client-ssh machine")
    if remote_server and args.server_host in ("127.0.0.1", "localhost"):
        parser.error("with a remote server, set --server-host to its IP address")
    if args.topics_root is None and not (remote_server and args.remote_topics):
        parser.error("--topics-root is required (or --remote-topics when the server runs elsewhere)")
    topics_root = args.topics_root.expanduser().resolve() if args.topics_root else None
    if topics_root is not None and not remote_server and not topics_root.is_dir():
        parser.error(f"topics root is not a directory: {topics_root}")
    if topics_root is not None and topics_root.is_dir() and not (topics_root / args.topic).is_dir():
        parser.error(f"topic directory does not exist: {topics_root / args.topic}")
    clients = parse_csv_list(args.client_counts, int)
    probabilities = parse_csv_list(args.failure_probabilities, float)
    if args.repeat < 1 or any(c < 1 for c in clients):
        parser.error("repeat and client counts must be positive")
    if any(p < 0 or p > 1 for p in probabilities):
        parser.error("failure probabilities must be between 0 and 1")
    if args.parallel_connections < 1 or args.parallel_connections > 64:
        parser.error("parallel connections must be in 1..64")

    only = {x.strip() for x in args.only.split(",") if x.strip()}
    if not only <= {"part1", "case1", "case2", "enhanced"}:
        parser.error("--only takes a comma-separated subset of: part1,case1,case2,enhanced")
    if not args.no_warmup:
        warm_cache(args, topics_root, args.topic)
    rows: list[dict[str, Any]] = list(existing)
    next_port = args.port_start
    # rows are written after EVERY scenario, so a crash or Ctrl-C hours into a lab session loses nothing
    for count in clients:
        if "part1" in only:
            for mode in ("single", "multi"):
                for rep in range(1, args.repeat + 1):
                    rows.append(run_one_scenario("Part1", mode, count, 0.0, rep, next_port,
                                                 topics_root, args.topic, args))
                    write_rows(CSV_PATH, rows); next_port += 101
        for key, case in (("case1", "Part2_Case1"), ("case2", "Part2_Case2"), ("enhanced", "Part2_Case2_Enhanced")):
            if key not in only:
                continue
            for p in probabilities:
                for rep in range(1, args.repeat + 1):
                    rows.append(run_one_scenario(case, "multi", count, p, rep, next_port,
                                                 topics_root, args.topic, args))
                    write_rows(CSV_PATH, rows); next_port += 101
    write_rows(CSV_PATH, rows)
    generate_graphs(read_rows(CSV_PATH))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

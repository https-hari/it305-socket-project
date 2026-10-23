#!/usr/bin/env python3
"""Self-narrating demo of the whole project on ONE machine (for the viva, rehearsals and sanity checks).

    python3 tools/demo.py --topics-root /path/to/raw-img [--topic elefante] [--clients 4] [--quick]

Four acts, each prints what it is about to show and why, then a result table:
  1  Part I     single-threaded vs multi-threaded server
  2  Part II    Case 1 (restart) vs Case 2 (resume) vs Enhanced under the SAME random connection drops
  3  Enhanced   why parallel connections help when a single connection is the bottleneck
  4  Resume     kill a client process in the middle of a transfer, restart it, watch it continue

Acts 1 and 3 route traffic through tools/netem_proxy.py (a per-connection bandwidth cap plus a little delay)
because on loopback a single connection is never the bottleneck. That emulation is announced when it is used.
For the real three-computer demonstration see docs/LAB_AND_VIVA_GUIDE.md.
"""
import argparse, os, re, shutil, signal, socket, subprocess, sys, tempfile, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
B, G, Y, C, D, N = "\033[1m", "\033[32m", "\033[33m", "\033[36m", "\033[2m", "\033[0m"
if not sys.stdout.isatty(): B = G = Y = C = D = N = ""
_port = [24000 + (os.getpid() % 3000) * 5]
procs = []

def nextport():
    while True:                      # skip ports that are already in use
        _port[0] += 1
        probe = socket.socket()
        try:
            probe.bind(("0.0.0.0", _port[0])); return _port[0]
        except OSError:
            continue
        finally:
            probe.close()

def say(msg=""): print(msg, flush=True)
def act(n, title, why):
    say(f"\n{B}{C}━━ Act {n}: {title}{N}"); say(f"{D}{why}{N}")

def wait_port(p):
    for _ in range(200):
        try: socket.create_connection(("127.0.0.1", p), timeout=0.2).close(); return
        except OSError: time.sleep(0.05)
    raise RuntimeError(f"nothing is listening on port {p}")

def topic_stats(root, topic):
    d = os.path.join(root, topic); files = checks = size = 0
    for f in os.listdir(d):
        fp = os.path.join(d, f)
        if os.path.isfile(fp) and not f.startswith("."):
            sz = os.path.getsize(fp); files += 1; size += sz; checks += 1 + max(1, -(-sz // 65536))
    return files, size, checks

def kv(stdout):
    for line in stdout.splitlines():
        if line.startswith("RESULT,"):
            return dict(x.split("=", 1) for x in line.split(",")[1:] if "=" in x)
    return {}

class Run:
    """A server (optionally behind the emulation proxy) plus helpers to launch clients against it."""
    def __init__(self, part, root, sargs, link=None):
        self.part, self.root = part, root
        self.port = nextport(); self.log = tempfile.mktemp(suffix=".log")
        self.srv = subprocess.Popen([f"{ROOT}/{part}/server", str(self.port), root] + sargs,
                                    stdout=open(self.log, "w"), stderr=subprocess.DEVNULL)
        wait_port(self.port); self.target = self.port; self.proxy = None
        if link:
            self.target = nextport()
            self.proxy = subprocess.Popen([sys.executable, f"{ROOT}/tools/netem_proxy.py", "--listen-port", str(self.target),
                                           "--target", f"127.0.0.1:{self.port}", "--delay-ms", str(link[0]),
                                           "--rate-mbit", f"{link[1]:.2f}"], stdout=subprocess.DEVNULL)
            wait_port(self.target)
    def client(self, topic, out, extra=()):
        return subprocess.Popen([f"{ROOT}/{self.part}/client", "127.0.0.1", str(self.target), topic, out] + list(extra),
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    def stop(self):
        if self.proxy: self.proxy.terminate()
        self.srv.send_signal(signal.SIGINT)
        try: self.srv.wait(timeout=20)
        except subprocess.TimeoutExpired: self.srv.kill()
        txt = open(self.log).read(); os.unlink(self.log)
        return sum(int(x) for x in re.findall(r"SERVER_METRIC,case=\d+,data_bytes=(\d+)", txt))

def scenario(part, root, sargs, topic, n=1, cargs=(), link=None):
    """run n concurrent clients; returns (wall seconds, [client result dicts], server data bytes)"""
    r = Run(part, root, sargs, link); out = tempfile.mkdtemp(prefix="demo_")
    t0 = time.time()
    cl = [r.client(topic, f"{out}/{i}", cargs) for i in range(n)]
    res = [kv(c.communicate(timeout=900)[0]) for c in cl]
    wall = time.time() - t0
    sent = r.stop(); shutil.rmtree(out, ignore_errors=True)
    return wall, res, sent

def mb(x): return f"{float(x) / 1e6:8.1f}"
def table(head, rows):
    w = [max(len(str(r[i])) for r in [head] + rows) for i in range(len(head))]
    say("  " + "  ".join(f"{B}{h:<{w[i]}}{N}" for i, h in enumerate(head)))
    for r in rows: say("  " + "  ".join(f"{str(c):<{w[i]}}" for i, c in enumerate(r)))

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topics-root", required=True); ap.add_argument("--topic")
    ap.add_argument("--clients", type=int, default=4); ap.add_argument("--quick", action="store_true", help="shorter transfers")
    a = ap.parse_args(); root = os.path.abspath(a.topics_root)
    topics = sorted(d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)))
    if not topics: sys.exit("no topic folders under " + root)
    topic = a.topic or min(topics, key=lambda t: topic_stats(root, t)[1])
    files, size, checks = topic_stats(root, topic)
    for part in ("Part1", "Part2_Case1", "Part2_Case2", "Part2_Case2_Enhanced"):
        if not os.path.exists(f"{ROOT}/{part}/server"): sys.exit(f"{part}/server is not built: run `make` first")
    say(f"{B}IT305 demo{N}   topic '{topic}': {files} files, {size / 1e6:.1f} MB, ~{checks} fault-injection checks   clients: {a.clients}")
    target_s = 2.5 if a.quick else 4.0
    rate_one = max(10.0, min(size * 8 / target_s / 1e6, 300.0 / a.clients))      # Mbit/s per connection for acts 1 and 3
    n = a.clients

    act(1, "Part I - single-threaded vs multi-threaded server",
        f"{n} clients fetch the topic at once. Each connection is capped at {rate_one:.0f} Mbit/s (emulated, 1 ms delay): a lone client\n"
        "never fills the server, so serving clients one after another wastes capacity that concurrent threads can use.")
    rows = []
    for mode in ("single", "multi"):
        wall, res, sent = scenario("Part1", root, [mode], topic, n, link=(1, rate_one))
        useful = sum(int(r.get("useful_payload_bytes", 0)) for r in res)
        rows.append([f"{mode}-threaded", f"{wall:6.2f} s", f"{useful / 1e6 / wall:7.1f} MB/s", f"{sum(1 for r in res if r)}/{n}"])
    table(["server", "wall time", "aggregate rate", "clients ok"], rows)

    p = min(0.01, 3.0 / checks)
    reps = 2 if a.quick else 3
    act(2, "Part II - Case 1 vs Case 2 vs Enhanced under random connection drops",
        f"The server resets each connection with probability {p:.2g} per check (~{checks} checks), about 3 drops per full transfer (mean of {reps} runs).\n"
        "Case 1 restarts from byte 0 after every drop; Case 2 and Enhanced resume from the byte where they stopped.")
    rows = []
    for label, part, extra in (("Case 1  (restart)", "Part2_Case1", []), ("Case 2  (resume)", "Part2_Case2", []),
                               ("Enhanced (K=4)", "Part2_Case2_Enhanced", ["4"])):
        acc = {"wall": 0.0, "drops": 0.0, "recv": 0.0, "useful": 0.0, "red": 0.0, "sent": 0.0}
        for _ in range(reps):
            wall, res, sent = scenario(part, root, [f"{p:.8f}"], topic, 1, extra); r = res[0]
            acc["wall"] += wall; acc["sent"] += sent; acc["drops"] += int(r.get("retries", 0))
            acc["recv"] += int(r.get("received_payload_bytes", 0)); acc["useful"] += int(r.get("useful_payload_bytes", 0))
            acc["red"] += int(r.get("redundant_payload_bytes", 0))
        k = {x: v / reps for x, v in acc.items()}
        rows.append([label, f"{k['wall']:6.2f} s", f"{k['drops']:.1f}", mb(k["recv"]), mb(k["useful"]), mb(k["red"]), mb(k["sent"])])
    table(["design", "time", "drops", "received MB", "useful MB", "redundant MB", "server-sent MB"], rows)
    say(f"{D}  redundant = bytes the client had to receive again; server-sent also counts data still queued in the kernel when a connection was reset.{N}")

    act(3, "Enhanced - why parallel connections",
        f"One connection capped at 60 Mbit/s (emulated, 2 ms delay) is the bottleneck. K parallel connections each get their own 60 Mbit/s,\n"
        "and requests are pipelined so a connection never waits a round trip between files.")
    rows = []; base = None
    for label, part, extra in (("Case 2 (1 connection)", "Part2_Case2", []), ("Enhanced K=1", "Part2_Case2_Enhanced", ["1"]),
                               ("Enhanced K=2", "Part2_Case2_Enhanced", ["2"]), ("Enhanced K=4", "Part2_Case2_Enhanced", ["4"])):
        wall, res, _ = scenario(part, root, ["0"], topic, 1, extra, link=(2, 60.0))
        base = base or wall
        rows.append([label, f"{wall:6.2f} s", f"{base / wall:5.2f}x"])
    table(["design", "time", "speed-up"], rows)

    act(4, "Resume after a client crash",
        "Case 2 client is killed with SIGKILL partway through (no goodbye, no cleanup), then started again with the same command.")
    r = Run("Part2_Case2", root, ["0"], link=(1, max(rate_one, 30.0))); out = tempfile.mkdtemp(prefix="demo_kill_")
    td = f"{out}/{topic}"
    c = r.client(topic, out)
    while c.poll() is None:
        have = sum(os.path.getsize(os.path.join(td, f)) for f in os.listdir(td) if not f.startswith(".")) if os.path.isdir(td) else 0
        if have >= size * 0.4: c.send_signal(signal.SIGKILL); break
        time.sleep(0.01)
    c.communicate()
    have = sum(os.path.getsize(os.path.join(td, f)) for f in os.listdir(td) if not f.startswith(".")) if os.path.isdir(td) else 0
    c2 = r.client(topic, out); res = kv(c2.communicate()[0]); r.stop()
    got = int(res.get("received_payload_bytes", 0))
    say(f"  killed with {have / 1e6:.1f} MB of {size / 1e6:.1f} MB on disk; the restarted client received only "
        f"{B}{G}{got / 1e6:.1f} MB{N} more (a restart from zero would need {size / 1e6:.1f} MB).")
    shutil.rmtree(out, ignore_errors=True)
    say(f"\n{B}Done.{N} These numbers come from one machine; the lab measurements use three computers (see docs/LAB_AND_VIVA_GUIDE.md).")

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: pass

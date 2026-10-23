#!/usr/bin/env python3
"""End-to-end verification of the four cases. Run `make` in every Part*/ folder first.

    python3 tools/verify.py <topics_root_dir> [--topic cane] [--quick]

Checks (each prints PASS/FAIL):
  1. every case downloads byte-identical files with several concurrent clients, with and without faults
  2. topic names match case-insensitively; unknown topics fail cleanly
  3. Part I / Case 1 servers refuse CMD_RESUME (no session management there)
  4. a client killed with SIGKILL mid-transfer resumes (Case 2, Enhanced) and receives fewer bytes the 2nd time
  5. SIGINT drains in-flight transfers instead of cutting them off
"""
import argparse, filecmp, os, shutil, signal, socket, struct, subprocess, sys, tempfile, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
results = []
_port = [20000 + (os.getpid() % 2000) * 10]

def port():
    """next TCP port that is actually free (a stale listener must not make a check fail or hang)"""
    while True:
        _port[0] += 1
        probe = socket.socket()
        try:
            probe.bind(("0.0.0.0", _port[0])); return _port[0]
        except OSError:
            continue
        finally:
            probe.close()

def report(name, ok, detail=""):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  -- {detail}" if detail else ""))

def same_dir(a, b):
    ca = sorted(f for f in os.listdir(a) if not f.startswith("."))
    cb = sorted(f for f in os.listdir(b) if not f.startswith("."))
    return ca == cb and all(filecmp.cmp(os.path.join(a, f), os.path.join(b, f), shallow=False) for f in ca)

def fault_checks(root, topics):
    """Largest number of fault-injection checks (1 per file start + 1 per 64 KB chunk) among the topics."""
    worst = 1
    for t in topics:
        d = os.path.join(root, t); n = 0
        for f in os.listdir(d):
            fp = os.path.join(d, f)
            if os.path.isfile(fp) and not f.startswith("."): n += 1 + max(1, -(-os.path.getsize(fp) // 65536))
        worst = max(worst, n)
    return worst

def result_of(stdout):
    for line in stdout.splitlines():
        if line.startswith("RESULT,"):
            return dict(kv.split("=", 1) for kv in line.split(",")[1:] if "=" in kv)
    return {}

def start_server(part, root, args, quiet=True):
    p = port()
    errlog = tempfile.mktemp(suffix=".err")
    srv = subprocess.Popen([f"{ROOT}/{part}/server", str(p), root] + args, stdout=subprocess.PIPE,
                           stderr=open(errlog, "w"), text=True)
    srv.errlog = errlog
    for _ in range(100):                       # wait until it accepts connections
        try:
            socket.create_connection(("127.0.0.1", p), timeout=0.2).close(); break
        except OSError:
            time.sleep(0.05)
    return srv, p

def stop_server(srv):
    srv.send_signal(signal.SIGINT)
    try: out, _ = srv.communicate(timeout=15)
    except subprocess.TimeoutExpired: srv.kill(); out, _ = srv.communicate()
    try:
        srv.faults = open(srv.errlog).read().count("FAULT_INJECTION"); os.unlink(srv.errlog)
    except OSError:
        srv.faults = -1
    return out

def run_clients(part, root, args, topics, extra=(), timeout=180):
    srv, p = start_server(part, root, args)
    out = tempfile.mkdtemp(prefix="verify_")
    procs = [subprocess.Popen([f"{ROOT}/{part}/client", "127.0.0.1", str(p), t, f"{out}/{i}"] + list(extra),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for i, t in enumerate(topics)]
    ok, rs = 0, []
    t0 = time.time()
    for i, (t, c) in enumerate(zip(topics, procs)):
        try: so, _ = c.communicate(timeout=timeout)
        except subprocess.TimeoutExpired: c.kill(); so = ""
        rs.append(result_of(so))
        src = os.path.join(root, t.lower() if not os.path.isdir(os.path.join(root, t)) else t)
        ok += (c.returncode == 0 and os.path.isdir(f"{out}/{i}/{t}") and same_dir(src, f"{out}/{i}/{t}"))
    wall = time.time() - t0
    stop_server(srv); shutil.rmtree(out, ignore_errors=True)
    run_clients.last_faults = srv.faults          # faults the server injected during this run
    return ok, rs, wall

def raw_request(p, mtype, payload=b""):
    s = socket.create_connection(("127.0.0.1", p), timeout=5)
    s.sendall(struct.pack(">BBHI", mtype, 0, 0, len(payload)) + payload)
    hdr = s.recv(8); s.close()
    return hdr[0] if len(hdr) == 8 else None

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("root"); ap.add_argument("--topic", default="cane")
    ap.add_argument("--quick", action="store_true"); a = ap.parse_args()
    root = os.path.abspath(a.root)
    topics = sorted(d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)))[:6]
    topics = topics or [a.topic]
    # A transfer with N fault checks survives untouched with probability (1-p)^N, so scale p to the topic size:
    # Case 1 (restart from zero) needs p*N ~ 1, the resuming cases can take p*N ~ 15 faults per transfer.
    N = fault_checks(root, topics)
    p1 = min(0.002, 1.0 / N); p2 = min(0.02, 15.0 / N)
    print(f"largest topic has ~{N} fault checks -> Case 1 p={p1:.2g}, resume cases p={p2:.2g}")
    cases = [("Part1", ["single"], []), ("Part1", ["multi"], []), ("Part2_Case1", [f"{p1:.8f}"], []),
             ("Part2_Case2", [f"{p2:.8f}"], []), ("Part2_Case2_Enhanced", [f"{p2:.8f}"], []),
             ("Part2_Case2_Enhanced", [f"{p2:.8f}", "buffered"], ["1"])]
    # 1 --- correctness matrix
    for part, sargs, cargs in cases:
        ok, rs, wall = run_clients(part, root, sargs, topics, cargs)
        report(f"{part} {' '.join(sargs)} {('K=' + cargs[0]) if cargs else ''} x{len(topics)} clients byte-identical",
               ok == len(topics), f"{ok}/{len(topics)} ok, {wall:.2f}s, retries(client0)={rs[0].get('retries', '?')}")
        if part != "Part1":     # every fault the server injects must show up as exactly one reconnect at the clients
            seen = sum(int(r.get("retries", 0)) for r in rs); injected = run_clients.last_faults
            report(f"  counters agree: server injected {injected} faults, clients counted {seen} reconnects", injected == seen)
    # 2 --- case-insensitive topic, unknown topic
    ok, _, _ = run_clients("Part1", root, ["multi"], [a.topic.upper()])
    report("topic lookup is case-insensitive (CANE -> cane)", ok == 1)
    srv, p = start_server("Part1", root, ["multi"])
    c = subprocess.run([f"{ROOT}/Part1/client", "127.0.0.1", str(p), "no_such_topic", tempfile.mkdtemp()], capture_output=True, text=True)
    stop_server(srv)
    report("unknown topic -> client exits non-zero", c.returncode != 0)
    # 3 --- RESUME refused where there is no session management
    for part, sargs in (("Part1", ["multi"]), ("Part2_Case1", ["0"])):
        srv, p = start_server(part, root, sargs)
        pl = struct.pack(">IIQ", 4, 5, 0) + b"cane" + b"x.jpg"
        t = raw_request(p, 0x02, pl); stop_server(srv)
        report(f"{part} server refuses CMD_RESUME", t == 0x12, f"reply opcode={t!r}")
    # 4 --- client killed mid-transfer, then resumed
    big = tempfile.mkdtemp(prefix="verify_big_"); os.makedirs(f"{big}/bigtopic")
    for i in range(8): open(f"{big}/bigtopic/f{i}.bin", "wb").write(os.urandom(12 * 1024 * 1024))
    for part, extra in (("Part2_Case2", []), ("Part2_Case2_Enhanced", ["2"])):
        done = False
        for attempt in range(6):
            srv, p = start_server(part, big, ["0"]); out = tempfile.mkdtemp(prefix="verify_crash_")
            c = subprocess.Popen([f"{ROOT}/{part}/client", "127.0.0.1", str(p), "bigtopic", out] + extra,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            td = f"{out}/bigtopic"; killed = False
            while c.poll() is None:
                n = len([f for f in os.listdir(td)]) if os.path.isdir(td) else 0
                if n >= 3 and os.path.exists(f"{td}/.checkpoint.tmp"):
                    c.send_signal(signal.SIGKILL); killed = True; break
                time.sleep(0.001)
            c.communicate()
            if not killed: stop_server(srv); shutil.rmtree(out, ignore_errors=True); continue
            had = sum(os.path.getsize(os.path.join(td, f)) for f in os.listdir(td) if not f.startswith("."))
            c2 = subprocess.run([f"{ROOT}/{part}/client", "127.0.0.1", str(p), "bigtopic", out] + extra, capture_output=True, text=True)
            r = result_of(c2.stdout); stop_server(srv)
            total = 8 * 12 * 1024 * 1024
            got = int(r.get("received_payload_bytes", total))
            ident = same_dir(f"{big}/bigtopic", td) and not os.path.exists(f"{td}/.checkpoint.tmp")
            report(f"{part}: SIGKILL mid-transfer then restart resumes", c2.returncode == 0 and ident and got < total,
                   f"had {had/1e6:.1f} MB on disk, 2nd run received {got/1e6:.1f} of {total/1e6:.1f} MB")
            shutil.rmtree(out, ignore_errors=True); done = True; break
        if not done: report(f"{part}: SIGKILL mid-transfer then restart resumes", False, "could not interrupt in time")
    # 5 --- graceful shutdown drains in-flight transfers
    srv, p = start_server("Part1", big, ["multi"]); out = tempfile.mkdtemp(prefix="verify_drain_")
    c = subprocess.Popen([f"{ROOT}/Part1/client", "127.0.0.1", str(p), "bigtopic", out], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    time.sleep(0.05); srv.send_signal(signal.SIGINT)
    c.communicate(timeout=60); sout, _ = srv.communicate(timeout=30)
    report("SIGINT drains in-flight client instead of cutting it off", c.returncode == 0 and same_dir(f"{big}/bigtopic", f"{out}/bigtopic")
           and "[SHUTDOWN]" in sout)
    shutil.rmtree(big, ignore_errors=True); shutil.rmtree(out, ignore_errors=True)
    print(f"\n{sum(results)}/{len(results)} checks passed")
    return 0 if all(results) else 1

if __name__ == "__main__":
    sys.exit(main())

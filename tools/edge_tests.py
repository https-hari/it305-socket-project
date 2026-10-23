#!/usr/bin/env python3
"""Robustness / edge-case suite (complements tools/verify.py). Needs `make` first; builds its own small datasets.

    python3 tools/edge_tests.py [--quick]

Covers: awkward file names and sizes, empty topics, hidden files, one huge file and thousands of tiny files under
random faults, 40 concurrent clients, reusing an output folder, a server that is killed and restarted in the middle
of a transfer, a server that is not there, failure probability 1.0, malformed / hostile network input and path
traversal attempts.  Every check prints PASS / FAIL."""
import argparse, os, random, shutil, signal, socket, struct, subprocess, sys, tempfile, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify as V

ROOT = V.ROOT
results = []
def report(name, ok, detail=""):
    results.append(ok); print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  -- {detail}" if detail else ""), flush=True)

def src_files(d):
    """what the server is meant to send: regular files (hidden ones included), no symlinks, no sub-folders"""
    return sorted(f for f in os.listdir(d) if os.path.isfile(os.path.join(d, f)) and not os.path.islink(os.path.join(d, f)))

def same(src, out):
    if not os.path.isdir(out): return False
    got = sorted(f for f in os.listdir(out) if f != ".checkpoint.tmp")
    if got != src_files(src): return False
    return all(open(os.path.join(src, f), "rb").read() == open(os.path.join(out, f), "rb").read() for f in got)

def build(root, quick):
    random.seed(11); os.makedirs(root)
    open(f"{root}/readme.txt", "w").write("not a topic")
    os.makedirs(f"{root}/empty")
    d = f"{root}/special"; os.makedirs(d)
    for name, size in [("file with spaces.jpg", 1000), ("ünïcode_名前.jpg", 2000), ("quote'and\"double.bin", 3000), ("a" * 255, 10),
                       ("zero.bin", 0), ("one.bin", 1), ("b65535", 65535), ("b65536", 65536), ("b65537", 65537), ("b131072", 131072),
                       ("UPPER.JPG", 5000), ("-leading-dash", 7), ("semi;colon&amp.bin", 9)]:
        open(os.path.join(d, name), "wb").write(os.urandom(size))
    d = f"{root}/messy"; os.makedirs(d)
    for name, size in [("normal.bin", 5000), (".hidden", 100), (".DS_Store", 50)]: open(f"{d}/{name}", "wb").write(os.urandom(size))
    os.makedirs(f"{d}/subdir"); open(f"{d}/subdir/inner.bin", "wb").write(b"x")
    os.symlink("normal.bin", f"{d}/link_to_file"); os.symlink("/nonexistent", f"{d}/broken_link")
    os.makedirs(f"{root}/big"); open(f"{root}/big/huge.bin", "wb").write(os.urandom((12 if quick else 40) * 1024 * 1024))
    os.makedirs(f"{root}/many")
    for i in range(1200 if quick else 2500): open(f"{root}/many/{i:05d}.dat", "wb").write(os.urandom(random.randint(1, 120)))

def serve(part, root, sargs):
    return V.start_server(part, root, sargs)

def fetch(part, root, topic, sargs, cargs=(), timeout=120, n=1):
    srv, p = serve(part, root, sargs); out = tempfile.mkdtemp(prefix="edge_")
    cl = [subprocess.Popen([f"{ROOT}/{part}/client", "127.0.0.1", str(p), topic, f"{out}/{i}"] + list(cargs),
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for i in range(n)]
    rcs, oks = [], 0
    for i, c in enumerate(cl):
        try: c.communicate(timeout=timeout)
        except subprocess.TimeoutExpired: c.kill(); c.communicate()
        rcs.append(c.returncode); oks += (c.returncode == 0 and same(os.path.join(root, topic), f"{out}/{i}/{topic}"))
    V.stop_server(srv); shutil.rmtree(out, ignore_errors=True)
    return oks, rcs

def start_proxy(target_port, rate_mbit, delay_ms=0):
    pp = V.port()
    px = subprocess.Popen([sys.executable, f"{ROOT}/tools/netem_proxy.py", "--listen-port", str(pp), "--target", f"127.0.0.1:{target_port}",
                           "--delay-ms", str(delay_ms), "--rate-mbit", str(rate_mbit)], stdout=subprocess.DEVNULL)
    for _ in range(100):
        try: socket.create_connection(("127.0.0.1", pp), timeout=0.2).close(); break
        except OSError: time.sleep(0.05)
    return px, pp

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--quick", action="store_true"); a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="edge_data_"); root = f"{tmp}/root"; build(root, a.quick)
    CASES = [("Part1", ["single"], []), ("Part1", ["multi"], []), ("Part2_Case1", ["0"], []), ("Part2_Case2", ["0"], []),
             ("Part2_Case2_Enhanced", ["0"], ["4"])]
    FAULTY = [("Part2_Case1", ["0.01"], []), ("Part2_Case2", ["0.05"], []), ("Part2_Case2_Enhanced", ["0.05"], ["4"])]

    # 1 --- awkward dataset contents, no faults and with faults
    for topic in ("special", "messy", "empty"):
        bad = [f"{p} {' '.join(s)}" for p, s, c in CASES if fetch(p, root, topic, s, c)[0] != 1]
        report(f"topic '{topic}' byte-identical in all cases", not bad, "; ".join(bad))
        bad = [p for p, s, c in FAULTY if fetch(p, root, topic, s, c)[0] != 1]
        report(f"topic '{topic}' byte-identical under random faults", not bad, "; ".join(bad))
    # 2 --- one huge file and thousands of tiny files under faults
    for topic, probs in (("big", (0.0015, 0.01, 0.01)), ("many", (0.0004, 0.004, 0.004))):
        for (p, s, c), prob in zip(FAULTY, probs):
            ok, rc = fetch(p, root, topic, [f"{prob}"], c, timeout=180)
            report(f"{p} '{topic}' p={prob} byte-identical", ok == 1, f"exit={rc}")
    # 3 --- 40 concurrent clients
    for p, s, c in (("Part1", ["multi"], []), ("Part2_Case2", ["0.001"], []), ("Part2_Case2_Enhanced", ["0.001"], ["4"])):
        ok, rc = fetch(p, root, "many", s, c, timeout=180, n=40)
        report(f"{p}: 40 concurrent clients all byte-identical", ok == 40, f"{ok}/40 ok")
    # 4 --- output folder reused / stale files
    for p, s, c in CASES[1:2] + CASES[3:5]:
        srv, port = serve(p, root, s); out = tempfile.mkdtemp(prefix="edge_")
        for _ in range(2): subprocess.run([f"{ROOT}/{p}/client", "127.0.0.1", str(port), "special", out] + c, capture_output=True)
        V.stop_server(srv)
        report(f"{p}: running twice into the same output folder", same(f"{root}/special", f"{out}/special")); shutil.rmtree(out)
    # 5 --- server killed (SIGKILL) and restarted in the middle of a transfer, clients must finish
    for part, extra in (("Part1", []), ("Part2_Case1", []), ("Part2_Case2", []), ("Part2_Case2_Enhanced", ["2"])):
        sargs = ["multi"] if part == "Part1" else ["0"]
        srv, sp = serve(part, root, sargs); px, pp = start_proxy(sp, 70)
        out = tempfile.mkdtemp(prefix="edge_"); td = f"{out}/big"
        c = subprocess.Popen([f"{ROOT}/{part}/client", "127.0.0.1", str(pp), "big", out] + extra, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        total = os.path.getsize(f"{root}/big/huge.bin"); killed = False
        while c.poll() is None:
            have = sum(os.path.getsize(os.path.join(td, f)) for f in os.listdir(td) if not f.startswith(".")) if os.path.isdir(td) else 0
            if have >= total * 0.3: srv.send_signal(signal.SIGKILL); srv.wait(); killed = True; break
            time.sleep(0.01)
        time.sleep(1.0)
        srv2 = subprocess.Popen([f"{ROOT}/{part}/server", str(sp), root] + sargs, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try: c.communicate(timeout=120)
        except subprocess.TimeoutExpired: c.kill(); c.communicate()
        srv2.send_signal(signal.SIGINT); srv2.wait(timeout=20); px.terminate()
        report(f"{part}: server killed and restarted mid-transfer, client completes", killed and c.returncode == 0 and same(f"{root}/big", td),
               f"killed={killed} exit={c.returncode}"); shutil.rmtree(out, ignore_errors=True)
    # 6 --- server missing, p = 1.0
    t0 = time.time(); r = subprocess.run([f"{ROOT}/Part2_Case2/client", "127.0.0.1", str(V.port()), "special", tempfile.mkdtemp()], capture_output=True, text=True, timeout=60)
    report("client with no server gives up (exit 1) within 15 s", r.returncode == 1 and time.time() - t0 < 15, f"{time.time() - t0:.1f}s")
    for part in ("Part2_Case1", "Part2_Case2", "Part2_Case2_Enhanced"):
        t0 = time.time(); ok, rc = fetch(part, root, "special", ["1.0"], timeout=120)
        report(f"{part}: failure probability 1.0 ends with an error, no hang", ok == 0 and rc[0] == 1 and time.time() - t0 < 100, f"exit={rc} {time.time() - t0:.1f}s")
    # 6b --- design-document features: SIGHUP topic reload, structured log file
    logf = f"{tmp}/server.log"
    live = f"{tmp}/live"; os.makedirs(f"{live}/alpha"); open(f"{live}/alpha/a.bin", "wb").write(os.urandom(5000))
    env = dict(os.environ, IT305_LOG_FILE=logf, IT305_LOG_LEVEL="debug")
    lp = V.port()
    srv = subprocess.Popen([f"{ROOT}/Part2_Case2/server", str(lp), live, "0.2"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    for _ in range(100):
        try: socket.create_connection(("127.0.0.1", lp), timeout=0.2).close(); break
        except OSError: time.sleep(0.05)
    def get(topic):
        out = tempfile.mkdtemp(); c = subprocess.run([f"{ROOT}/Part2_Case2/client", "127.0.0.1", str(lp), topic, out], capture_output=True, timeout=60)
        ok = c.returncode == 0 and same(f"{live}/{topic}", f"{out}/{topic}") if os.path.isdir(f"{live}/{topic}") else c.returncode != 0
        shutil.rmtree(out, ignore_errors=True); return ok
    before = get("beta")                                        # does not exist yet -> must fail
    os.makedirs(f"{live}/beta"); open(f"{live}/beta/b.bin", "wb").write(os.urandom(7000))
    srv.send_signal(signal.SIGHUP); time.sleep(0.5)
    added = get("beta")
    shutil.rmtree(f"{live}/alpha"); srv.send_signal(signal.SIGHUP); time.sleep(0.5)
    removed = not os.path.isdir(f"{live}/alpha") and get("alpha")
    report("SIGHUP: a topic folder added at run time becomes available (and a removed one disappears)", before and added and removed,
           f"unknown-before={before} served-after-HUP={added} gone-after-removal={removed}")
    # reload storm while clients are downloading (old registry generations must stay alive for them)
    os.makedirs(f"{live}/storm"); [open(f"{live}/storm/{i}.dat", "wb").write(os.urandom(random.randint(1, 80000))) for i in range(400)]
    srv.send_signal(signal.SIGHUP); time.sleep(0.4)
    stop = threading.Event()
    def hup():
        while not stop.is_set(): srv.send_signal(signal.SIGHUP); time.sleep(0.03)
    th = threading.Thread(target=hup); th.start()
    res = [get("storm") for _ in range(10)]
    stop.set(); th.join(); alive = srv.poll() is None
    report("SIGHUP storm during active downloads: every download correct, server alive", all(res) and alive, f"{sum(res)}/10 ok, alive={alive}")
    srv.send_signal(signal.SIGINT); srv.wait(timeout=20)
    import re as _re
    lines = open(logf).read().splitlines() if os.path.exists(logf) else []
    pat = _re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3} \[thread [0-9a-f]+\] \[(-|\d+\.\d+\.\d+\.\d+:\d+)\] [A-Z_]+ ")
    kinds = {ln.split("] ", 2)[2].split(" ")[0] for ln in lines if pat.match(ln)}
    report("log file has timestamped, thread- and client-tagged lines for the key events",
           lines and all(pat.match(ln) for ln in lines) and {"SERVER_START", "RELOAD", "FAULT_INJECTION", "RESUME", "CONNECT", "REQUEST", "CLOSE", "SERVER_STOP"} <= kinds,
           f"{len(lines)} lines, events seen: {sorted(kinds)}")
    # 7 --- hostile / malformed input, path traversal
    srv, p = serve("Part2_Case2_Enhanced", root, ["0"])
    def raw(data, wait=1.0):
        s = socket.create_connection(("127.0.0.1", p), timeout=5); s.sendall(data); s.settimeout(wait)
        try: r = s.recv(64)
        except OSError: r = b""
        s.close(); return r
    raw(os.urandom(3)); raw(struct.pack(">BBHI", 0x01, 0, 0, 0xFFFFFFFF)); raw(struct.pack(">BBHI", 0xEE, 0, 0, 0)); raw(struct.pack(">BBHI", 0x04, 0, 0, 5) + b"junk!")
    for bad_topic in (b"../root", b"special/../../etc", b"/etc", b""):
        r = raw(struct.pack(">BBHI", 0x01, 0, 0, len(bad_topic)) + bad_topic)
        report(f"server refuses topic {bad_topic!r}", len(r) >= 8 and r[0] in (0x10, 0x12), f"reply={r[:1].hex() or 'closed'}")
    pl = struct.pack(">IIQ", 7, 9, 0) + b"special" + b"../../x.b"
    r = raw(struct.pack(">BBHI", 0x02, 0, 0, len(pl)) + pl)
    report("server refuses a resume with a path-traversal file name", len(r) >= 8 and r[0] in (0x10, 0x12), f"reply={r[:1].hex() or 'closed'}")
    ok, rc = 0, []
    out = tempfile.mkdtemp(); c = subprocess.run([f"{ROOT}/Part2_Case2_Enhanced/client", "127.0.0.1", str(p), "special", out], capture_output=True)
    report("server still serves normally after all that abuse", c.returncode == 0 and same(f"{root}/special", f"{out}/special"))
    V.stop_server(srv)
    c = subprocess.run([f"{ROOT}/Part1/client", "127.0.0.1", "1", "../escape", tempfile.mkdtemp()], capture_output=True, text=True, timeout=30)
    report("client rejects a topic name containing '..' / '/'", c.returncode != 0)
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n{sum(results)}/{len(results)} checks passed"); return 0 if all(results) else 1

if __name__ == "__main__":
    sys.exit(main())

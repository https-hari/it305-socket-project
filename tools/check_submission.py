#!/usr/bin/env python3
"""Submission gate: checks everything the handout requires, then builds the archive and tests it from a fresh extract.

    python3 tools/check_submission.py [--group 095 120] [--allow-draft] [--no-zip]

PASS / WARN / FAIL per check. The archive FinalSubmission_Group_<XXX>_<YYY>.zip is only written when nothing FAILs.
--allow-draft downgrades "report still has placeholders / results missing" to WARN so the packaging itself can be rehearsed."""
import argparse, csv, filecmp, os, re, shutil, signal, socket, subprocess, sys, tempfile, time, zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARTS = ["Part1", "Part2_Case1", "Part2_Case2", "Part2_Case2_Enhanced"]
status = {"PASS": 0, "WARN": 0, "FAIL": 0}
def emit(level, msg, detail=""):
    status[level] += 1
    print(f"[{level}] {msg}" + (f"  -- {detail}" if detail else ""), flush=True)
def check(ok, msg, detail="", soft=False): emit("PASS" if ok else ("WARN" if soft else "FAIL"), msg, "" if ok else detail)

def text_of_pdf(path):
    try: return subprocess.run(["pdftotext", path, "-"], capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.TimeoutExpired): return None

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--group", nargs=2, default=["095", "120"]); ap.add_argument("--allow-draft", action="store_true")
    ap.add_argument("--no-zip", action="store_true"); a = ap.parse_args(); X, Y = a.group
    soft = a.allow_draft
    name = f"FinalSubmission_Group_{X}_{Y}"; report = f"FinalReport_Group_{X}_{Y}.pdf"
    check(bool(re.fullmatch(r"\d{3}", X) and re.fullmatch(r"\d{3}", Y)), f"group numbers are three digits ({X}, {Y})")

    print("\n-- required contents")
    check(os.path.isfile(f"{ROOT}/README.md"), "README.md present")
    check(os.path.isfile(f"{ROOT}/{report}"), f"{report} present")
    for d in PARTS:
        missing = [f for f in ("server.c", "client.c", "Makefile") if not os.path.isfile(f"{ROOT}/{d}/{f}")]
        check(not missing, f"{d}/ has server.c, client.c, Makefile", f"missing {missing}")
        com = f"{ROOT}/{d}/common"; mastercom = f"{ROOT}/common"
        if os.path.isdir(mastercom):
            check(os.path.isdir(com) and not filecmp.dircmp(mastercom, com).diff_files and not filecmp.dircmp(mastercom, com).left_only,
                  f"{d}/common is a current copy of common/ (self-contained folder)", "run ./sync_common.sh")
    check(os.path.isdir(f"{ROOT}/Results"), "Results/ present")

    print("\n-- README")
    readme = open(f"{ROOT}/README.md", encoding="utf-8").read() if os.path.isfile(f"{ROOT}/README.md") else ""
    for needle, what in (("Part1", "Part1 build/run"), ("Part2_Case1", "Case 1 build/run"), ("Part2_Case2_Enhanced", "Enhanced build/run"),
                         ("failure_probability", "failure-probability argument"), ("SO_LINGER", "fault-injection mechanism (SO_LINGER/RST)")):
        check(needle in readme, f"README mentions {what}", f"'{needle}' not found")

    print("\n-- report")
    md = open(f"{ROOT}/FinalReport_Group_{X}_{Y}.md", encoding="utf-8").read() if os.path.isfile(f"{ROOT}/FinalReport_Group_{X}_{Y}.md") else ""
    pdf = text_of_pdf(f"{ROOT}/{report}") if os.path.isfile(f"{ROOT}/{report}") else None
    for label, body in (("report .md", md), ("report PDF", pdf)):
        if body is None:
            emit("WARN", f"{label}: could not be read (pdftotext missing?)"); continue
        if not body: continue
        hits = {k: len(re.findall(k, body)) for k in (r"\bTBD\b", r"\[Fill in", r"DRAFT TEMPLATE", r"\bTODO\b", r"Generated from the lab measurements")}
        left = {k: v for k, v in hits.items() if v}
        check(not left, f"{label} has no leftover placeholders", f"{left}", soft=soft)
    if md:
        for sec in ("Part I", "Case 1", "Case 2", "Enhanced"):
            check(sec in md, f"report mentions {sec}")
        check(bool(re.search(r"\b2024\d{5}\b", md)), "report cover lists roll numbers")

    print("\n-- results")
    csvp = f"{ROOT}/Results/raw_metrics.csv"; rows = []
    if os.path.isfile(csvp):
        rows = [r for r in csv.DictReader(open(csvp, newline="")) if not r.get("label")]
    ok_rows = [r for r in rows if r.get("success") == "1"]
    check(len(ok_rows) > 0, "Results/raw_metrics.csv has successful measurements", f"{len(rows)} rows", soft=soft)
    if ok_rows:
        for case, mode in (("Part1", "single"), ("Part1", "multi"), ("Part2_Case1", "multi"), ("Part2_Case2", "multi"), ("Part2_Case2_Enhanced", "multi")):
            g = [r for r in ok_rows if r["case"] == case and r["mode"] == mode]
            counts = sorted({int(r["client_count"]) for r in g}); probs = sorted({float(r["failure_probability"]) for r in g})
            check(len(g) > 0, f"measurements for {case} ({mode})", soft=soft)
            check(len(counts) >= 3, f"  {case} {mode}: at least 3 client counts", f"have {counts}", soft=True)
            if case != "Part1":
                check(0.0 in probs and len(probs) >= 3, f"  {case}: failure probability 0 and at least 2 others", f"have {probs}", soft=True)
            reps = {}
            for r in g: reps[(r["client_count"], r["failure_probability"])] = reps.get((r["client_count"], r["failure_probability"]), 0) + 1
            check(not reps or min(reps.values()) >= 2, f"  {case} {mode}: every setting repeated at least twice", "single runs are noisy", soft=True)
    names = ["Part1_throughput.png"] + [f"{c}_{k}.png" for c in PARTS[1:] for k in ("throughput", "overhead")]
    for n in names:
        check(os.path.isfile(f"{ROOT}/Results/{n}") or os.path.isfile(f"{ROOT}/Results/{n[:-4]}.pdf"), f"graph Results/{n}", soft=soft)

    print("\n-- archive")
    if status["FAIL"]:
        print(f"\n{status['FAIL']} FAIL: fix them first, the archive was not written."); return 1
    if a.no_zip:
        print("\n--no-zip: stopping before the archive."); return 0
    stage = tempfile.mkdtemp(prefix="submit_"); top = f"{stage}/{name}"
    skip_dirs = {".git", "__pycache__", "node_modules"}
    def ignore(d, names):
        out = [n for n in names if n in skip_dirs or n.endswith((".o", ".pyc", ".zip", ".tar.gz"))]
        if os.path.basename(d) in PARTS: out += [n for n in names if n in ("server", "client")]      # no Linux binaries in the archive
        if os.path.basename(d) == "Results": out += [n for n in names if n.endswith("_output")]
        return out
    shutil.copytree(ROOT, top, ignore=ignore)
    for junk in ("Dockerfile", ".dockerignore"):
        pass                                                                                    # extras are kept on purpose
    big = [os.path.join(dp, f) for dp, _, fs in os.walk(top) for f in fs if os.path.getsize(os.path.join(dp, f)) > 20 * 1024 * 1024]
    check(not big, "no file in the archive is larger than 20 MB (no dataset, no downloads)", str(big))
    out_zip = os.path.join(os.path.dirname(ROOT), f"{name}.zip")
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for dp, ds, fs in os.walk(top):
            ds.sort(); z.write(dp, os.path.relpath(dp, stage) + "/")
            for f in sorted(fs):
                p = os.path.join(dp, f); z.writestr(zipfile.ZipInfo.from_file(p, os.path.relpath(p, stage)), open(p, "rb").read(), zipfile.ZIP_DEFLATED)
    check(True, f"archive written: {out_zip} ({os.path.getsize(out_zip) / 1e6:.1f} MB)")
    shutil.rmtree(stage, ignore_errors=True)

    print("\n-- fresh-extract test (what the grader will do)")
    ex = tempfile.mkdtemp(prefix="extract_")
    with zipfile.ZipFile(out_zip) as z: z.extractall(ex)
    tops = os.listdir(ex); check(tops == [name], "archive has exactly one top-level folder named like the archive", str(tops))
    base = f"{ex}/{name}"
    if sys.platform != "linux":
        emit("WARN", "not Linux: skipping the build and run test of the extracted archive (do it on a lab PC)")
    else:
        data = tempfile.mkdtemp(prefix="grader_data_"); os.makedirs(f"{data}/animal_a"); os.makedirs(f"{data}/animal_b")
        import random
        for t in ("animal_a", "animal_b"):
            for i in range(30): open(f"{data}/{t}/img{i}.jpg", "wb").write(os.urandom(random.randint(1, 150000)))
        for d in PARTS:
            before = set(os.listdir(f"{base}/{d}"))
            r = subprocess.run(["make"], cwd=f"{base}/{d}", capture_output=True, text=True)
            new = set(os.listdir(f"{base}/{d}")) - before
            check(r.returncode == 0 and {"server", "client"} <= new and not (new - {"server", "client"}),
                  f"{d}: `make` builds exactly server and client", f"rc={r.returncode} new files={sorted(new)}")
            if r.returncode != 0: continue
            port = 30000 + random.randint(0, 20000)
            sargs = ["0"] if d != "Part1" else []
            srv = subprocess.Popen([f"{base}/{d}/server", str(port), data] + sargs, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(60):
                try: socket.create_connection(("127.0.0.1", port), timeout=0.2).close(); break
                except OSError: time.sleep(0.1)
            out = tempfile.mkdtemp(); c = subprocess.run([f"{base}/{d}/client", "127.0.0.1", str(port), "animal_a", out], capture_output=True)
            srv.send_signal(signal.SIGINT); srv.wait(timeout=20)
            same = os.path.isdir(f"{out}/animal_a") and sorted(os.listdir(f"{out}/animal_a")) == sorted(os.listdir(f"{data}/animal_a")) and all(
                filecmp.cmp(f"{out}/animal_a/{f}", f"{data}/animal_a/{f}", shallow=False) for f in os.listdir(f"{data}/animal_a"))
            check(c.returncode == 0 and same, f"{d}: required CLI works (./server <port> <root>{' <p>' if d != 'Part1' else ''}, ./client <ip> <port> <topic> <outdir>)", f"client exit {c.returncode}")
            shutil.rmtree(out, ignore_errors=True)
    shutil.rmtree(ex, ignore_errors=True)
    print(f"\n{status['PASS']} passed, {status['WARN']} warnings, {status['FAIL']} failed")
    print(f"archive: {out_zip}")
    return 0 if status["FAIL"] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())

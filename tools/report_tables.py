#!/usr/bin/env python3
"""Turn Results/raw_metrics.csv into the report's tables, graph references and factual observations.

    python3 tools/report_tables.py --csv Results/raw_metrics.csv --report FinalReport_Group_095_120.md
    python3 tools/report_tables.py --csv Results/raw_metrics.csv --stdout          # just print everything

The report contains marker pairs  <!-- BEGIN:name --> ... <!-- END:name -->  (part1_table, part2_headline, part2_all,
vs_part1, ablation, observations, graphs); this script rewrites what is between them, so it can be re-run after every
new measurement. It only uses rows with success=1 and an empty `label` for the main tables (labelled rows are ablation
runs and appear in the `ablation` table). Numbers are means over the repeats (mean ± standard deviation when n > 1).
The observations are computed facts, not interpretation: read them, check them against the graphs, and write the analysis."""
import argparse, csv, os, re, statistics, sys
from collections import defaultdict

CASES = [("Part2_Case1", "Case 1"), ("Part2_Case2", "Case 2"), ("Part2_Case2_Enhanced", "Case 2 Enhanced")]
MB = 1e6

def num(r, k):
    try: return float(r[k])
    except (KeyError, ValueError, TypeError): return 0.0

def read(path):
    with open(path, newline="") as f: rows = list(csv.DictReader(f))
    for r in rows:
        r["_n"] = int(num(r, "client_count")); r["_p"] = num(r, "failure_probability")
    return rows

def mean(xs): return sum(xs) / len(xs) if xs else 0.0
def fmt(xs, nd=2):
    if not xs: return "–"
    if len(xs) == 1: return f"{xs[0]:.{nd}f}"
    return f"{mean(xs):.{nd}f} ± {statistics.stdev(xs):.{nd}f}"
def col(rows, k, scale=1.0): return [num(r, k) / scale for r in rows]
def pfmt(p): return "0" if p == 0 else f"{p:g}"

def group(rows, case, **kw):
    out = []
    for r in rows:
        if r["case"] != case: continue
        if any((r["_n"] if k == "n" else r["_p"] if k == "p" else r.get(k)) != v for k, v in kw.items()): continue
        out.append(r)
    return out

class Data:
    def __init__(self, rows):
        self.all = rows
        main = [r for r in rows if not r.get("label")]
        self.ok = [r for r in main if r.get("success") == "1"]
        self.bad = [r for r in main if r.get("success") != "1"]
        self.abl = [r for r in rows if r.get("label") and r.get("success") == "1"]
        self.counts = sorted({r["_n"] for r in main})
        self.probs = sorted({r["_p"] for r in main if r["case"] != "Part1"})
    def failed(self, case, n, p): return len(group(self.bad, case, n=n, p=p))

def tbl(head, rows, align=None):
    align = align or ["---"] + ["---:"] * (len(head) - 1)
    out = ["| " + " | ".join(head) + " |", "|" + "|".join(align) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)

def part1_table(d):
    rows = []
    for n in d.counts:
        s, m = group(d.ok, "Part1", n=n, mode="single"), group(d.ok, "Part1", n=n, mode="multi")
        if not s and not m: continue
        ratio = (mean(col(m, "aggregate_throughput_Bps")) / mean(col(s, "aggregate_throughput_Bps"))) if s and m and mean(col(s, "aggregate_throughput_Bps")) else 0
        rows.append([n, fmt(col(s, "aggregate_throughput_Bps", MB)), fmt(col(s, "server_aggregate_throughput_Bps", MB)),
                     fmt(col(m, "aggregate_throughput_Bps", MB)), fmt(col(m, "server_aggregate_throughput_Bps", MB)),
                     f"{ratio:.2f}×" if ratio else "–"])
    return tbl(["Clients", "Single: client-measured (MB/s)", "Single: server-measured (MB/s)", "Multi: client-measured (MB/s)",
                "Multi: server-measured (MB/s)", "Multi ÷ single"], rows) if rows else "_no Part I rows in the CSV_"

def part2_rows(d, n):
    rows = []
    for case, label in CASES:
        for p in d.probs:
            g = group(d.ok, case, n=n, p=p); nf = d.failed(case, n, p)
            if not g and not nf: continue
            useful = mean(col(g, "useful_payload_bytes")); red = mean(col(g, "redundant_payload_bytes")); sent = mean(col(g, "server_data_bytes_sent"))
            rows.append([label, pfmt(p), f"{len(g)}/{len(g) + nf}", fmt(col(g, "client_elapsed_sec_max")), fmt(col(g, "retries"), 1),
                         fmt(col(g, "received_payload_bytes", MB), 1), fmt(col(g, "useful_payload_bytes", MB), 1),
                         f"{red / MB:.1f} ({100 * red / useful:.0f}%)" if useful else "–",
                         f"{sent / MB:.1f} ({100 * (sent - useful) / useful:+.0f}%)" if useful and sent else "–",
                         fmt(col(g, "aggregate_throughput_Bps", MB))])
    return rows
P2_HEAD = ["Case", "Failure prob.", "Runs finished", "Time (s)", "Reconnects", "Received (MB)", "Useful (MB)", "Redundant at client (MB, % of useful)",
           "Server-sent (MB, % over useful)", "Aggregate (MB/s)"]

def headline_clients(d, want):
    c = [n for n in d.counts if any(r["_n"] == n and r["case"] != "Part1" for r in d.ok)]
    if want in c: return want
    below = [n for n in c if n <= 4]
    return max(below) if below else (min(c) if c else None)

def part2_headline(d, n):
    if n is None: return "_no Part II rows in the CSV_"
    return f"Totals over all {n} concurrent client(s); means over the repeats.\n\n" + tbl(P2_HEAD, part2_rows(d, n))

def part2_all(d, skip):
    parts = []
    for n in d.counts:
        if n == skip: continue
        rows = part2_rows(d, n)
        if rows: parts.append(f"**{n} concurrent client(s)**\n\n" + tbl(P2_HEAD, rows))
    return "\n\n".join(parts) if parts else "_(only one client count was measured)_"

def vs_part1(d):
    rows = []
    for n in d.counts:
        base = group(d.ok, "Part1", n=n, mode="multi")
        if not base: continue
        b = mean(col(base, "aggregate_throughput_Bps", MB))
        row = [n, f"{b:.2f}"]
        for case, _ in CASES:
            g = group(d.ok, case, n=n, p=0.0)
            v = mean(col(g, "aggregate_throughput_Bps", MB)) if g else 0
            row.append(f"{v:.2f} ({100 * v / b:.0f}%)" if g and b else "–")
        rows.append(row)
    return tbl(["Clients", "Part I multi-threaded (MB/s)", "Case 1, p=0 (MB/s, % of Part I)", "Case 2, p=0 (MB/s, % of Part I)",
                "Enhanced, p=0 (MB/s, % of Part I)"], rows) if rows else "_needs Part I and Part II rows with failure probability 0_"

def ablation(d):
    if not d.abl: return "_no labelled (ablation) runs in the CSV. Run e.g. `--only enhanced --label K1 --parallel-connections 1 --append`_"
    g = defaultdict(list)
    for r in d.abl: g[(r["case"], r.get("label"), r.get("parallel_connections"), r.get("server_data_path"), r["_n"], r["_p"])].append(r)
    rows = []
    for (case, label, k, path, n, p), rs in sorted(g.items(), key=lambda kv: (kv[0][4], kv[0][5], str(kv[0][1]))):
        ref = group(d.ok, "Part2_Case2_Enhanced", n=n, p=p); c2 = group(d.ok, "Part2_Case2", n=n, p=p)
        rows.append([label, k or "–", path or "–", n, pfmt(p), fmt(col(rs, "aggregate_throughput_Bps", MB)), fmt(col(rs, "client_elapsed_sec_max")),
                     fmt(col(rs, "retries"), 1), fmt(col(ref, "aggregate_throughput_Bps", MB)) if ref else "–", fmt(col(c2, "aggregate_throughput_Bps", MB)) if c2 else "–"])
    return tbl(["Label", "K", "Data path", "Clients", "Failure prob.", "Throughput (MB/s)", "Time (s)", "Reconnects",
                "Enhanced main run (MB/s)", "Case 2 (MB/s)"], rows, ["---"] * 3 + ["---:"] * 7)

def observations(d, hn):
    o = []
    for n in d.counts:
        s, m = group(d.ok, "Part1", n=n, mode="single"), group(d.ok, "Part1", n=n, mode="multi")
        if s and m:
            a, b = mean(col(s, "aggregate_throughput_Bps", MB)), mean(col(m, "aggregate_throughput_Bps", MB))
            if a: o.append(f"Part I, {n} client(s): multi-threaded {b:.1f} MB/s vs single-threaded {a:.1f} MB/s (**{b / a:.2f}×**).")
    if hn is not None:
        for p in [x for x in d.probs if x > 0]:
            c1, c2, en = (group(d.ok, c, n=hn, p=p) for c, _ in CASES)
            if c1 and c2:
                u1, u2 = mean(col(c1, "useful_payload_bytes")), mean(col(c2, "useful_payload_bytes"))
                t1, t2 = mean(col(c1, "client_elapsed_sec_max")), mean(col(c2, "client_elapsed_sec_max"))
                s = (f"p={pfmt(p)}, {hn} client(s): Case 1 received **{mean(col(c1, 'received_payload_bytes')) / u1:.2f}×** the useful bytes "
                     f"(redundant {100 * mean(col(c1, 'redundant_payload_bytes')) / u1:.0f}%) and took **{t1 / t2:.2f}×** as long as Case 2; "
                     f"Case 2 redundant at the client {100 * mean(col(c2, 'redundant_payload_bytes')) / u2:.0f}%")
                sent = mean(col(c2, "server_data_bytes_sent"))
                if sent and u2: s += f", but the server wrote {100 * (sent - u2) / u2:+.0f}% more than the useful bytes (data lost in flight at resets)"
                o.append(s + ".")
            if c2 and en:
                a, b = mean(col(c2, "aggregate_throughput_Bps", MB)), mean(col(en, "aggregate_throughput_Bps", MB))
                if a: o.append(f"p={pfmt(p)}, {hn} client(s): Enhanced {b:.1f} MB/s vs Case 2 {a:.1f} MB/s (**{b / a:.2f}×**).")
    for case, label in CASES:
        for n in d.counts:
            for p in d.probs:
                f = d.failed(case, n, p)
                if f: o.append(f"**{label} did not complete in {f} of {f + len(group(d.ok, case, n=n, p=p))} run(s)** at p={pfmt(p)} with {n} client(s) (counted as failed, excluded from averages).")
    for n in d.counts:
        base = group(d.ok, "Part1", n=n, mode="multi")
        for case, label in CASES[1:]:
            g = group(d.ok, case, n=n, p=0.0)
            if base and g:
                b, v = mean(col(base, "aggregate_throughput_Bps")), mean(col(g, "aggregate_throughput_Bps"))
                if b: o.append(f"No-fault cost of the machinery, {n} client(s): {label} reaches **{100 * v / b:.0f}%** of Part I multi-threaded throughput.")
    return "\n".join(f"- {x}" for x in o) if o else "_not enough rows to compute observations_"

def graphs(results_dir):
    names = [("Part1_throughput.png", "Part I: aggregate throughput vs number of clients")]
    for case, label in CASES:
        names += [(f"{case}_throughput.png", f"{label}: useful throughput vs failure probability"),
                  (f"{case}_overhead.png", f"{label}: overhead vs failure probability")]
    out = [f"![{cap}]({results_dir}/{fn}){{ width=80% }}\n" for fn, cap in names if os.path.exists(os.path.join(results_dir, fn))]
    return "\n".join(out) if out else "_no graphs yet: run `python3 Results/benchmark.py --graphs-only`_"

def replace(text, name, body):
    pat = re.compile(rf"(<!-- BEGIN:{name} -->)(.*?)(<!-- END:{name} -->)", re.S)
    if not pat.search(text): raise SystemExit(f"marker pair '{name}' not found in the report")
    return pat.sub(lambda m: f"{m.group(1)}\n{body}\n{m.group(3)}", text, count=1)

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default="Results/raw_metrics.csv"); ap.add_argument("--report"); ap.add_argument("--stdout", action="store_true")
    ap.add_argument("--headline-clients", type=int, default=4, help="client count for the main Part II table (default 4, or the closest available)")
    ap.add_argument("--results-dir", default="Results")
    ap.add_argument("--clear", action="store_true", help="reset every generated block in --report to a placeholder (use after trial runs)")
    a = ap.parse_args()
    if a.clear:
        if not a.report: raise SystemExit("--clear needs --report")
        text = open(a.report, encoding="utf-8").read()
        for k in ("part1_table", "part2_headline", "part2_all", "vs_part1", "ablation", "observations", "graphs"):
            text = replace(text, k, "_Generated from the lab measurements: run `tools/build_report.sh` after the experiments._")
        open(a.report, "w", encoding="utf-8").write(text); print("cleared generated blocks in", a.report); return 0
    d = Data(read(a.csv)); hn = headline_clients(d, a.headline_clients)
    blocks = {"part1_table": part1_table(d), "part2_headline": part2_headline(d, hn), "part2_all": part2_all(d, hn), "vs_part1": vs_part1(d),
              "ablation": ablation(d), "observations": observations(d, hn), "graphs": graphs(a.results_dir)}
    if a.stdout or not a.report:
        for k, v in blocks.items(): print(f"\n===== {k} =====\n{v}")
        return 0
    text = open(a.report, encoding="utf-8").read()
    for k, v in blocks.items(): text = replace(text, k, v)
    open(a.report, "w", encoding="utf-8").write(text)
    print(f"updated {a.report} from {a.csv}: {len(d.ok)} good rows, {len(d.bad)} failed rows, {len(d.abl)} ablation rows; headline client count {hn}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

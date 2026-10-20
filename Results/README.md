# Results and performance analysis

`raw_metrics.csv` is a column header/template only until the three-computer experiments have been run. Use actual measurements; do not copy smoke-test timing numbers into the report as lab results.

## Running the experiments

`raw_metrics.csv` is a column header/template until the three-computer experiments have been run. Use actual
measurements; do not copy smoke-test timing numbers into the report as lab results (a single machine over loopback says
nothing about a LAN).

The full command lines for the ssh / manual three-computer setup and for the ablation runs are in the top-level
`README.md`, section 8. For a quick local smoke test of the metric collection and graph generation:

```bash
python3 Results/benchmark.py --topics-root /absolute/path/to/topics_root --topic cane \
  --client-counts 1,2 --failure-probabilities 0,0.0001 --repeat 1
```

Graphs written: `Part1_throughput.png` (single vs multi), and for each Part II case `<case>_throughput.png`
(useful throughput vs failure probability, one line per client count) and `<case>_overhead.png` (redundant bytes the
client received, and the server-sent excess, as a percentage of useful payload). Delete any graphs produced by a
smoke test before the final run so that only real lab results remain in this folder.

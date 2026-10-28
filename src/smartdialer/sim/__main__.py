"""python -m smartdialer.sim [options]

Runs scenarios (default A-D) x providers (A, B) x modes (progressive, predictive) against the
local PostgreSQL, in parallel worker processes (each gets its own throwaway *_sim database), and
writes a Markdown report plus JSON.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from smartdialer.sim import report
from smartdialer.sim.parallel import init_worker, run_in_process
from smartdialer.sim.runner import RunSpec, run_scenario
from smartdialer.sim.scenarios import DEFAULT_KEYS, SCENARIOS

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m smartdialer.sim", description=__doc__)
    ap.add_argument("--scenarios", nargs="+", choices=sorted(SCENARIOS), help="default: A B C D")
    ap.add_argument("--all", action="store_true", help="also run the failure-injection scenarios E and F")
    ap.add_argument("--providers", nargs="+", default=["A", "B"], choices=["A", "B"])
    ap.add_argument("--modes", nargs="+", default=["PROGRESSIVE", "PREDICTIVE"], type=str.upper,
                    choices=["PROGRESSIVE", "PREDICTIVE"])
    ap.add_argument("--agents", type=int, help="override the scenario's agent count (default 100)")
    ap.add_argument("--duration", type=int, help="override the scenario's length in virtual seconds")
    ap.add_argument("--quick", action="store_true", help="40 agents, 300 s: a fast smoke run")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--epsilon", type=float, help="STRESS TEST: loosen the controller's overflow-risk limit (default 0.01) to watch the abandon budget and fallbacks take over")
    ap.add_argument("--parallel", type=int, default=min(4, os.cpu_count() or 1), help="worker processes")
    ap.add_argument("--out", default="docs/SIMULATION_RESULTS.md")
    ap.add_argument("--json", default="docs/sim_results.json")
    args = ap.parse_args(argv)

    keys = args.scenarios or (list(SCENARIOS) if args.all else list(DEFAULT_KEYS))
    agents, duration = (40, 300) if args.quick else (args.agents, args.duration)
    specs = [RunSpec(k, p, m, args.seed, agents, duration, epsilon=args.epsilon) for k in keys for p in args.providers for m in args.modes]
    print(f"{len(specs)} runs on {args.parallel} process(es) ...", file=sys.stderr)

    started, results = time.time(), []
    if args.parallel <= 1:
        for spec in specs:
            results.append(asyncio.run(run_scenario(spec)))
            print(f"  done {spec.scenario}/{spec.provider}/{spec.mode} in {results[-1]['wall_seconds']}s", file=sys.stderr)
    else:
        ctx = mp.get_context("spawn")
        counter = ctx.Value("i", 0)
        with ProcessPoolExecutor(args.parallel, mp_context=ctx, initializer=init_worker, initargs=(counter,)) as ex:
            futures = {ex.submit(run_in_process, s): s for s in specs}
            for f in as_completed(futures):
                s, r = futures[f], f.result()
                results.append(r)
                print(f"  done {s.scenario}/{s.provider}/{s.mode} in {r['wall_seconds']}s "
                      f"({len(results)}/{len(specs)})", file=sys.stderr)

    results.sort(key=lambda r: (r["scenario"], r["provider"], r["mode"]))
    print(report.console(results))
    for path, text in ((args.out, report.markdown(results)), (args.json, json.dumps(results, indent=1, default=str))):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text, encoding="utf-8")
    print(f"\nwrote {args.out} and {args.json}  (total wall time {time.time() - started:.0f}s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

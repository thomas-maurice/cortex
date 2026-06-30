#!/usr/bin/env python3
"""Render a before/after comparison of two recall_accuracy result files.

Usage:
  recall_compare.py --title "Local long-seed" scripts/results/baseline-local.json scripts/results/chunked-local.json
"""
import argparse
import json


def load(path):
    return json.load(open(path))["metrics"]


def pct(x):
    return f"{x * 100:5.1f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", default="Recall comparison")
    ap.add_argument("baseline")
    ap.add_argument("chunked")
    args = ap.parse_args()

    b = load(args.baseline)
    c = load(args.chunked)

    print(f"\n## {args.title}  (n={b['n']} queries)\n")
    print(f"| Metric | Baseline ({b['label']}) | Chunked ({c['label']}) | Δ |")
    print("|---|---:|---:|---:|")
    for k in ["recall@1", "recall@3", "recall@5", "recall@10", "mrr"]:
        bv, cv = b[k], c[k]
        delta = cv - bv
        arrow = "▲" if delta > 1e-9 else ("▼" if delta < -1e-9 else "—")
        fmt = pct if k.startswith("recall") else (lambda x: f"{x:.3f}")
        print(f"| {k} | {fmt(bv)} | {fmt(cv)} | {arrow} {fmt(abs(delta))} |")
    print(f"| found / missed | {b['found']}/{b['missed']} | {c['found']}/{c['missed']} | |")
    mr_b = f"{b['mean_rank_found']:.2f}" if b["mean_rank_found"] else "—"
    mr_c = f"{c['mean_rank_found']:.2f}" if c["mean_rank_found"] else "—"
    print(f"| mean rank (found) | {mr_b} | {mr_c} | (lower is better) |")

    print("\n### By needle depth (recall@5)\n")
    depths = [d for d in ("start", "middle", "end") if d in b.get("by_depth", {})]
    print("| Depth | Baseline r@5 | Chunked r@5 | Δ | Baseline MRR | Chunked MRR |")
    print("|---|---:|---:|---:|---:|---:|")
    for d in depths:
        bd, cd = b["by_depth"][d], c["by_depth"][d]
        delta = cd["recall@5"] - bd["recall@5"]
        arrow = "▲" if delta > 1e-9 else ("▼" if delta < -1e-9 else "—")
        print(
            f"| {d} (n={bd['n']}) | {pct(bd['recall@5'])} | {pct(cd['recall@5'])} | "
            f"{arrow} {pct(abs(delta))} | {bd['mrr']:.3f} | {cd['mrr']:.3f} |"
        )


if __name__ == "__main__":
    main()

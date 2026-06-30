#!/usr/bin/env python3
"""Measure retrieval recall accuracy of Cortex Search against a labeled query set.

For each labeled query it calls the Search RPC (Connect JSON over HTTP) and
records the rank at which the EXPECTED memory id appears in the results, then
computes recall@k and MRR. It is a black-box client of a running server, so the
same harness measures the store before and after a code change (e.g. chunking).

Query file format: a JSON array of objects with at least:
  { "id": "...", "query": "...", "expectedId": "<memory uuid>", "depth": "start|middle|end" }
("depth" is optional; used only for the per-depth breakdown.)

Usage:
  recall_accuracy.py --server http://localhost:8088 \
      --queries testdata/recall-queries.json \
      --label baseline-local --out scripts/results/baseline-local.json \
      [--token TOK] [--limit 10] [--namespace '*']
"""
import argparse
import json
import os
import statistics
import sys
import time
import urllib.request


def search(server, token, query, namespace, limit):
    url = server.rstrip("/") + "/cortex.v1.MemoryService/Search"
    body = json.dumps(
        {"query": query, "namespace": namespace, "limit": limit, "noReinforce": True}
    ).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def rank_of(hits, expected_id):
    """1-based rank of expected_id among hit memory ids, or None if absent."""
    for i, h in enumerate(hits):
        if h.get("memory", {}).get("id") == expected_id:
            return i + 1
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="http://localhost:8088")
    ap.add_argument("--token", default=os.environ.get("CORTEX_TOKEN", ""))
    ap.add_argument("--queries", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--namespace", default="*")
    args = ap.parse_args()

    queries = json.load(open(args.queries))
    rows = []
    ranks = []
    for q in queries:
        resp = {"hits": []}
        for attempt in range(2):  # one retry on transient error
            try:
                resp = search(args.server, args.token, q["query"], args.namespace, args.limit)
                break
            except Exception as e:  # noqa: BLE001 - harness, surface and continue
                if attempt == 1:
                    print(f"ERROR query {q.get('id')}: {e}", file=sys.stderr)
                else:
                    time.sleep(1)
        hits = resp.get("hits", []) or []
        rk = rank_of(hits, q["expectedId"])
        ranks.append(rk)
        rows.append(
            {
                "id": q.get("id"),
                "expectedId": q["expectedId"],
                "depth": q.get("depth"),
                "rank": rk,
                "distance": hits[rk - 1].get("distance") if rk else None,
                "topId": hits[0]["memory"]["id"] if hits else None,
            }
        )

    n = len(ranks) or 1

    def recall_at(k):
        return sum(1 for r in ranks if r is not None and r <= k) / n

    found = [r for r in ranks if r]
    metrics = {
        "label": args.label,
        "n": len(ranks),
        "recall@1": recall_at(1),
        "recall@3": recall_at(3),
        "recall@5": recall_at(5),
        "recall@10": recall_at(10),
        "mrr": sum(1.0 / r for r in found) / n,
        "found": len(found),
        "missed": len(ranks) - len(found),
        "mean_rank_found": statistics.mean(found) if found else None,
    }

    # Per-depth breakdown — the key signal for chunking: deep needles benefit most.
    depths = {}
    for q, r in zip(queries, ranks):
        depths.setdefault(q.get("depth", "?"), []).append(r)
    metrics["by_depth"] = {
        d: {
            "n": len(rs),
            "recall@5": sum(1 for x in rs if x and x <= 5) / len(rs),
            "mrr": sum(1.0 / x for x in rs if x) / len(rs),
        }
        for d, rs in sorted(depths.items())
    }

    print(json.dumps(metrics, indent=2))
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        json.dump({"metrics": metrics, "rows": rows}, open(args.out, "w"), indent=2)
        print("wrote", args.out)


if __name__ == "__main__":
    main()

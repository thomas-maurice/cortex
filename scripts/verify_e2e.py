#!/usr/bin/env python3
"""End-to-end signal harness for a running cortex stack.

Exercises the full path against the live Connect RPC API and emits hard metrics
with PASS/FAIL gates — not just check marks:

  save -> index latency   (NATS -> Ollama embed -> Weaviate, by the worker)
  search latency p50/p95   over N paraphrased queries
  ranking quality          top score + margin to the runner-up for the target
  reinforcement            accessCount delta across searches ("living memory")
  export roundtrip         List count vs Status count, target present
  delete                   store returns to baseline

Leaves the store exactly as it found it. Usage: python3 scripts/verify_e2e.py
Env: CORTEX_BASE (default http://localhost:8088), N_SEARCH (default 20).
"""
import json
import os
import statistics
import sys
import time
import urllib.request

BASE = os.environ.get("CORTEX_BASE", "http://localhost:8088")
N_SEARCH = int(os.environ.get("N_SEARCH", "20"))
SVC = "/cortex.v1.MemoryService"
# Unique, non-dictionary marker so search must match by meaning, never by luck.
MARK = f"verify-{int(time.time())}-zephyr-quokka-meridian"


def rpc(method: str, body: dict):
    """One Connect RPC. Returns (parsed_json, elapsed_seconds)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{BASE}{SVC}/{method}",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as r:
        payload = json.loads(r.read())
    return payload, time.perf_counter() - t0


def count() -> int:
    return int(rpc("Status", {})[0]["memoryCount"])


def fail(msg: str):
    print(f"  \033[31mFAIL\033[0m {msg}")
    fail.hit = True


fail.hit = False


def gate(ok: bool, msg: str):
    print(f"  {'\033[32mPASS\033[0m' if ok else '\033[31mFAIL\033[0m'} {msg}")
    if not ok:
        fail.hit = True


def main():
    print(f"== cortex e2e signal harness :: {BASE} ==")
    st = rpc("Status", {})[0]
    print(f"version={st['version']} model={st['model']} dims={st['dims']} "
          f"deps: nats={st['natsOk']} weaviate={st['weaviateOk']} ollama={st['ollamaOk']}")
    base = int(st["memoryCount"])
    print(f"baseline memoryCount={base}\n")

    # 1) SAVE + measure save->index latency by polling for count == base+1.
    save, _ = rpc("Save", {
        "text": f"End-to-end proof memory: the magic codename is {MARK}; "
                f"it concerns cortex release verification.",
        "namespace": "global", "tags": ["proof", "e2e"],
    })
    mid = save["id"]
    try:
        run_checks(mid, base)
    finally:
        # Always remove the probe memory, even if a gate raised — never leak.
        rpc("Delete", {"id": mid})
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < 15 and count() != base:
            time.sleep(0.1)
        gate(count() == base, f"store restored to baseline {base}")

    print()
    if fail.hit:
        print("\033[31mRESULT: FAIL\033[0m")
        sys.exit(1)
    print("\033[32mRESULT: PASS — full pipeline verified, store untouched\033[0m")


def run_checks(mid: str, base: int):
    # 1) save->index latency by polling for count == base+1.
    t0 = time.perf_counter()
    idx_ms = None
    while time.perf_counter() - t0 < 30:
        if count() == base + 1:
            idx_ms = (time.perf_counter() - t0) * 1000
            break
        time.sleep(0.1)
    gate(idx_ms is not None, f"save->index: indexed in {idx_ms:.0f} ms" if idx_ms
         else "save->index: NOT indexed within 30s")

    # 2) SEARCH latency distribution over N paraphrased queries (no marker text).
    q = "what is the secret magic codename for the release check"
    lat = []
    for _ in range(N_SEARCH):
        _, dt = rpc("Search", {"query": q, "namespace": "global", "limit": 5})
        lat.append(dt * 1000)
    p50 = statistics.median(lat)
    p95 = sorted(lat)[min(len(lat) - 1, int(len(lat) * 0.95))]
    print(f"  search latency: p50={p50:.0f}ms p95={p95:.0f}ms "
          f"min={min(lat):.0f} max={max(lat):.0f} (n={N_SEARCH})")
    gate(p50 < 500, f"search p50 {p50:.0f}ms < 500ms")

    # 3) RANKING quality: marker query must rank the target #1 with clear margin.
    res, _ = rpc("Search", {"query": q, "namespace": "global", "limit": 5})
    hits = res.get("hits", [])
    rank = next((i for i, h in enumerate(hits) if h["memory"]["id"] == mid), -1)
    # Connect omits a zero-value float, so an absent "distance" means 0.0 (a
    # perfect hit) — NOT a far one. Default to 0.0, never 1.0.
    top_score = 1 - hits[0].get("distance", 0.0) if hits else 0
    margin = ((hits[1].get("distance", 0.0) - hits[0].get("distance", 0.0))
              if len(hits) > 1 else float("inf"))
    print(f"  ranking: target rank={rank} topScore={top_score:.3f} "
          f"margin_to_#2={margin:.3f}")
    gate(rank == 0, "target is the #1 semantic hit")

    # 4) REINFORCEMENT ("living memory"): accessCount climbs as searches recall it.
    counts = []
    for _ in range(3):
        rpc("Search", {"query": q, "namespace": "global", "limit": 1})
        time.sleep(0.3)  # reinforce is async best-effort
        r, _ = rpc("Search", {"query": q, "namespace": "global", "limit": 1})
        counts.append(r["hits"][0]["memory"].get("accessCount", 0))
    print(f"  reinforcement: accessCount progression {counts}")
    gate(counts[-1] > counts[0], f"accessCount grew {counts[0]}->{counts[-1]}")

    # 5) EXPORT roundtrip: List (the backup export path) must match Status + carry target.
    # allLimit (10000) is the server's full-store cap; Weaviate rejects more.
    lst, _ = rpc("List", {"namespace": "*", "limit": 10000})
    mems = lst.get("memories", [])
    present = any(MARK in m.get("text", "") for m in mems)
    cur = count()
    gate(len(mems) == cur, f"export count {len(mems)} == Status count {cur}")
    gate(present, "export carries the target memory")
    # (delete + baseline restore happens in main's finally — leak-proof)


if __name__ == "__main__":
    main()

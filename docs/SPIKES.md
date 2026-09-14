# Spikes

Branches that got measured and did not get merged, and why.

---

## `spike/ttl-cache` — a grace-period cache after the flight completes

**The idea.** `@unicall(ttl=0.02)` keeps a finished flight in the registry
for 20ms instead of evicting it the moment it completes, so a burst that
arrives just after the flight finished reuses the cached result instead of
paying for a brand new one.

**Measured.** Two waves of 200 concurrent callers, 5ms apart:

```
no ttl:     2 executions
ttl=0.02s:  1 execution
```

A real halving, for a burst that lands inside the window. Full script:
[`benchmarks/ttl_spike.py`](../benchmarks/ttl_spike.py).

**Why it stays a branch.** The benefit is real but the semantics it buys are
not what `@unicall()` currently promises. Read on its own, `@unicall()` says
one thing: concurrent calls with the same arguments never duplicate work.
`ttl=` quietly adds a second thing — the answer you get back may be up to
`ttl` seconds old, from a call that already returned before yours started.
For a decorator that is otherwise about *correctness under concurrency*, not
*staleness*, that is a meaningfully bigger thing to bury in one keyword
argument.

It also blurs `in_flight()`: with `ttl` set, an entry in the registry can be
a currently-running flight or a finished one still waiting to be evicted, and
the method can no longer tell the caller which.

If the actual goal is "cache this result for N seconds," that already has a
name and a much larger design space — invalidation, size limits, per-key TTL
— that belongs in an actual cache, not smuggled into a concurrency primitive
whose whole value is doing exactly one thing.

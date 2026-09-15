# unicall

**One call for everyone waiting on the same thing — an async decorator that
collapses concurrent calls into a single flight, in one process or across
many.**

[![CI](https://github.com/i-safonoff/unicall/actions/workflows/ci.yml/badge.svg)](https://github.com/i-safonoff/unicall/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![asyncio](https://img.shields.io/badge/asyncio-native-009688)
![Redis](https://img.shields.io/badge/redis-optional-DC382D)
![Tests](https://img.shields.io/badge/tests-25%20passing-brightgreen)
![Typed](https://img.shields.io/badge/typed-py.typed-informational)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## Contents

- [Why I built this](#why-i-built-this)
- [What it does](#what-it-does)
- [The six things the local version is really about](#the-six-things-the-local-version-is-really-about)
- [Going distributed](#going-distributed)
- [The branches](#the-branches)
- [Quickstart](#quickstart)
- [Testing it](#testing-it)
- [Project layout](#project-layout)
- [What I would do differently at scale](#what-i-would-do-differently-at-scale)

## Why I built this

A thundering herd is a two-line story: a cache entry expires, a thousand
requests land in the same second, and a thousand identical queries hit
whatever is behind the cache at once. The fix is a one-line idea too —
"if a call for this key is already running, wait for it instead of starting
your own" — and I wanted the version of that idea I could actually stand
behind under `asyncio`, not the version that looks right until the first
caller times out.

It did not look right on the first try. `await shared_task` from more than
one caller works fine until someone's timeout fires — then it turns out
awaiting someone else's task hands them your cancellation too, and the
flight dies for everyone who was patiently waiting on it. That bug, and the
five smaller ones next to it, are what the local version is actually about.

Local coalescing only ever solves one process's problem, though: two workers
behind a load balancer each run their own flight for the same key, because
nothing connects their registries. The second half of this project is that
problem — one call across every process that's asking, coordinated through
Redis — and it broke in ways the local version never could, starting with a
fast flight finishing before a second caller even tried to join it.

## What it does

```python
from unicall import unicall

@unicall()
async def get_price(symbol: str) -> float:
    return await exchange.fetch_price(symbol)

# 500 concurrent callers, one symbol, one process: one call to
# fetch_price, 500 callers get the same result.
results = await asyncio.gather(*(get_price("BTC") for _ in range(500)))
```

```python
from redis.asyncio import Redis
from unicall import distributed
from unicall.backends.redis import RedisBackend

backend = RedisBackend(Redis.from_url("redis://localhost:6379/0"))

@distributed(backend, lease=5.0)
async def get_price(symbol: str) -> float:
    return await exchange.fetch_price(symbol)

# Same call, four worker processes behind a load balancer: one call to
# fetch_price total, not one per process.
```

## The six things the local version is really about

### 1. Awaiting a shared task hands it your cancellation

`await shared_task` from more than one caller looks fine until one of them
is wrapped in `asyncio.wait_for(...)`. A timeout cancels *that caller's own
task*, and `Task.cancel()` cancels whatever the task is currently blocked on
— which, at the point of the timeout, is the shared flight itself. One
caller's timeout kills the result for every other caller who never asked to
be cancelled. Fixed by awaiting the flight through `asyncio.shield()`
instead, which lets a caller detach without taking the flight down.
[`test_cancellation.py`](tests/test_cancellation.py)

### 2. A flight nobody stayed to await still needs its exception read

`shield()` solves problem 1 by letting the flight keep running after a
caller detaches from it. That means it's possible for *every* caller to
cancel before the flight finishes — and if nothing ever calls `.exception()`
on the underlying task, asyncio logs "Task exception was never retrieved"
once it's garbage collected. The registry's own done-callback now retrieves
it unconditionally, whether or not anyone was still around to see it.

### 3. Registering a flight is safe only because nothing awaits in between

"Check if a flight exists for this key, else start one" is check-and-set —
the shape that needs a mutex under threads. Under `asyncio` it doesn't,
because only an `await` yields control to another coroutine, and there is no
`await` between the lookup and the insert. The trap is that this is
invisible in the code: nothing stops a future change from adding one
"for safety" and quietly reopening the exact race a lock would look like
it's preventing. [`docs/DECISIONS.md`](docs/DECISIONS.md#2-no-lock-around-registering-a-flight)

### 4. Two calls that mean the same thing must hash the same

`f(5)` and `f(x=5)` are the same call. Hashing `(args, kwargs)` as each
caller happened to write it treats them as two different keys — so they ran
twice. Fixed by binding both through the function's own `inspect.Signature`
before hashing, so positional and keyword calls land on the same key
regardless of how they were written. Discovered by a test that failed
against the first version. [`test_keys.py`](tests/test_keys.py)

### 5. Different keys must never wait on each other

The registry is a `dict` keyed per call, not one lock guarding all of it —
so three flights on three different keys run fully in parallel. Worth a
regression test on its own: a single shared lock around registration would
still pass every other test in this project and fail only this one.
[`test_isolation.py`](tests/test_isolation.py)

### 6. A key called once must not leak its flight forever

Cleanup that only happens "the next time this key is looked up" never
happens for a key that's called exactly once — its finished task just sits
in the registry, forever, one entry per distinct key ever seen instead of
per key currently in flight. Fixed by evicting from the done-callback
unconditionally, the moment the flight finishes, regardless of whether
anyone ever calls that key again. [`test_cleanup.py`](tests/test_cleanup.py)

## Going distributed

`@distributed(backend)` coordinates the same idea across processes through a
`Backend` (a `Protocol` — `RedisBackend` is the one this library ships).
Local coalescing keeps working exactly as above inside each process; this
adds a second layer on top: before a process actually runs the function, it
races every other process for leadership of the key.

```
   4 worker processes, one cold key
                                          local coalescing only: 4 executions
   ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐   (one per process)
   │ proc A │ │ proc B │ │ proc C │ │ proc D │
   └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘
       └──────────┴─────┬────┴──────────┘
                    Redis: SET NX PX
                    one lock, one winner        + distributed backend: 1 execution
```

Needs `pip install unicall[redis]`. Measured, not asserted:
[`benchmarks/distributed_stampede.py`](benchmarks/distributed_stampede.py) —
4 processes × 50 concurrent callers, 4 executions with local coalescing
alone, 1 with the backend coordinating them.

### The four things the distributed backend is really about

1. **A fast flight can finish before a follower even tries.** `SET NX PX`
   for the lock looks like the thing to check — until a function with no
   internal `await` acquires, runs, publishes, and releases all before a
   second caller's own `try_acquire` has even reached Redis. That caller
   finds no lock and wrongly concludes it should lead too. Found by a test
   that failed the first time it ran; fixed by checking for an already
   published *result* before every acquire attempt, not only while already
   waiting on someone else's flight — the result outlives the lock on
   purpose. [`docs/DECISIONS.md#10`](docs/DECISIONS.md#10-the-published-result-is-checked-before-every-acquire-attempt-not-only-while-waiting-on-someone-elses-flight)

2. **A lease needs a heartbeat, or leadership dies mid-run.** `lease=`
   has to outlast the function, or another process becomes leader while the
   first is still working — not corrupt, but no longer exclusive. A
   background task renews the lease every `lease / 3` seconds for as long
   as the function runs. [`test_lease_renewal_keeps_a_long_flight_exclusive`](tests/integration/test_distributed.py)

3. **Releasing a lock you might not own any more is a lock you can steal.**
   If a lease already expired, a delayed `release()` or `renew()` from its
   original owner could touch a lock a *different* process has since
   acquired — deleting someone else's lock, or extending a lease that isn't
   yours to extend. Both operations compare a token before touching
   anything, in a Lua script so the compare and the write are atomic — the
   Redlock safe-unlock pattern.
   [`test_safe_release_does_not_delete_a_lock_it_no_longer_owns`](tests/integration/test_distributed.py)

4. **An error can't cross a process boundary as itself.** A follower needs
   to know the leader's flight raised, but reconstructing the *original*
   exception from data that arrived over Redis means either guessing at its
   constructor or deserializing something expressive enough to build
   arbitrary objects — the same door pickle opens. Followers get
   `RemoteFlightError(type_name, message)` instead: honest about what
   actually crossed the wire.
   [`test_leaders_error_propagates_as_remote_flight_error`](tests/integration/test_distributed.py)

## The branches

| Branch | | |
|---|---|---|
| [`feat/cancellation-safety`](../../tree/feat/cancellation-safety) | merged | Local problems 1 and 2 — shield, and the exception nobody was left to retrieve |
| [`feat/custom-keys`](../../tree/feat/custom-keys) | merged | `key=`, a clear error on unhashable arguments, and local problem 4 |
| [`feat/metrics`](../../tree/feat/metrics) | merged | `Coalescer.stats()` and the `metrics=` hook |
| [`feat/distributed-backend`](../../tree/feat/distributed-backend) | merged | Everything under [Going distributed](#going-distributed) |
| [`spike/ttl-cache`](../../tree/spike/ttl-cache) | **not merged** | Measured a real benefit, rejected for what it would mean for what `@unicall()` promises |

The spike is worth the click: `ttl=` halved executions for a burst landing
right after a flight completed — a real, measured number — and still isn't
merged. Why: [`docs/SPIKES.md`](docs/SPIKES.md).

## Quickstart

```bash
git clone https://github.com/i-safonoff/unicall.git
cd unicall
pip install -e ".[dev]"
```

```python
import asyncio
from unicall import unicall

calls = 0

@unicall()
async def slow_lookup(key: str) -> str:
    global calls
    calls += 1
    await asyncio.sleep(0.05)
    return f"value for {key}"

async def main() -> None:
    results = await asyncio.gather(*(slow_lookup("a") for _ in range(50)))
    print(calls, "execution for", len(results), "callers")

asyncio.run(main())
```

Distributed, against a local Redis (`docker run -p 6379:6379 redis:7-alpine`):

```python
import asyncio
from redis.asyncio import Redis
from unicall import distributed
from unicall.backends.redis import RedisBackend

backend = RedisBackend(Redis.from_url("redis://localhost:6379/0"))

@distributed(backend)
async def slow_lookup(key: str) -> str:
    await asyncio.sleep(0.05)
    return f"value for {key}"

asyncio.run(slow_lookup("a"))
```

See the actual numbers for yourself:

```bash
python benchmarks/stampede.py
python benchmarks/ttl_spike.py
python benchmarks/distributed_stampede.py   # starts its own throwaway Redis
```

## Testing it

```bash
pytest -m "not integration"   # no Docker needed
pytest -m integration         # the distributed backend, against a real Redis
pytest                        # both
```

Every test drives real concurrency rather than mocking it — real `asyncio`
tasks and cancellation for the local version, a real Redis container
(testcontainers) for the distributed one. A mock scheduler would have let
the cancellation bug in local problem 1 pass silently: it only exists
because of exactly how `Task.cancel()` and `await` interact, which is the
one thing a mock can't reproduce by accident. Same logic for Redis: the
race in distributed problem 1 depends on exact `SET NX PX` and Lua-script
semantics that a fake would have to reimplement correctly to fake at all.

## Project layout

```
src/unicall/
├── __init__.py          the public API
├── _core.py              the local registry, the key, the shield, the eviction
├── _distributed.py       leadership, leases, RemoteFlightError, the Backend protocol
├── _metrics.py           Stats and the Metrics protocol
├── _util.py              stable_hash
└── backends/redis.py     the Redis Backend: SET NX PX, two Lua CAS scripts

tests/                    17 unit tests, real asyncio, no Docker needed
tests/integration/        8 tests against a real Redis (testcontainers)
benchmarks/stampede.py            1000 concurrent callers into one cold key
benchmarks/ttl_spike.py           the rejected spike, measured
benchmarks/distributed_stampede.py 4 processes, one cold key, coordinated
docs/DECISIONS.md         eleven decisions, with what each gave up
docs/SPIKES.md            the branch that was measured and not merged
```

## What I would do differently at scale

Honest limitations, not a roadmap:

- **No Pub/Sub fast path.** Followers poll ([`docs/DECISIONS.md#8`](docs/DECISIONS.md#8-followers-poll-for-a-result-they-dont-subscribe-to-one)); the
  latency floor is `poll_interval` (15ms by default) instead of Redis
  pushing a wakeup. Deliberate, not an oversight — but a lower-latency
  version would add Pub/Sub as an optimistic wakeup on top of the same poll
  loop as its correctness fallback, not replace polling with it.
- **One Redis, not a cluster.** Nothing here sends `CLUSTER`-aware key
  routing or handles a failover mid-lease. A lease that survives its
  Redis's own failover is a harder problem than this library takes on.
- **Lease-based coordination is best-effort, not exactly-once.** A process
  that loses its lease mid-run can still finish and hand its own local
  callers a correct answer, but nothing stops a *different* process from
  having taken over and produced a second, redundant execution in that
  window. A hard guarantee needs a fencing token enforced by the resource
  being protected, which this library has no way to require.
- **Redis is the only shipped `Backend`.** The `Protocol` doesn't care —
  Postgres advisory locks, etcd, anything with a compare-and-swap primitive
  would work — but only one is implemented and tested here.
- **`asyncio` only, still.** No thread-safe or cross-event-loop version of
  the *local* registry — but this no longer matters as much as it used to:
  the actual reason to want that was almost always "processes need to share
  one dedup," and that's what `distributed()` is for now.
- **`stable_hash` is only as good as `repr()`.** A custom object whose
  default repr includes its `id()` still defeats it, same as before.

## License

MIT — see [LICENSE](LICENSE).

# unicall

**One call for everyone waiting on the same thing — an async decorator that
collapses concurrent calls into a single flight.**

[![CI](https://github.com/i-safonoff/unicall/actions/workflows/ci.yml/badge.svg)](https://github.com/i-safonoff/unicall/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![asyncio](https://img.shields.io/badge/asyncio-only-009688)
![Tests](https://img.shields.io/badge/tests-10%20passing-brightgreen)
![Typed](https://img.shields.io/badge/typed-py.typed-informational)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## Contents

- [Why I built this](#why-i-built-this)
- [What it does](#what-it-does)
- [The six things this project is really about](#the-six-things-this-project-is-really-about)
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
five smaller ones next to it, are what this project is actually about.

## What it does

```python
from unicall import unicall

@unicall()
async def get_price(symbol: str) -> float:
    return await exchange.fetch_price(symbol)

# 500 concurrent callers, one symbol: one call to fetch_price, 500 callers
# get the same result.
results = await asyncio.gather(*(get_price("BTC") for _ in range(500)))
```

## The six things this project is really about

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

## The branches

| Branch | | |
|---|---|---|
| [`feat/cancellation-safety`](../../tree/feat/cancellation-safety) | merged | Problems 1 and 2 — shield, and the exception nobody was left to retrieve |
| [`feat/custom-keys`](../../tree/feat/custom-keys) | merged | `key=`, a clear error on unhashable arguments, and problem 4 |
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

See the actual numbers for yourself:

```bash
python benchmarks/stampede.py
python benchmarks/ttl_spike.py
```

## Testing it

```bash
pytest
```

Every test drives real `asyncio` concurrency — real tasks, real
cancellation, real timing — rather than mocking the event loop. A mock
scheduler would have let the cancellation bug in problem 1 pass silently:
the bug only exists because of exactly how `Task.cancel()` and `await`
interact, which is the one thing a mock can't reproduce by accident.

## Project layout

```
src/unicall/
├── __init__.py        public API: unicall, Coalescer, UnhashableArgumentsError
└── _core.py            the registry, the key, the shield, the eviction

tests/                  10 tests, all against a real event loop
benchmarks/stampede.py  1000 concurrent callers into one cold key
benchmarks/ttl_spike.py the spike, measured
docs/DECISIONS.md       five decisions, with what each gave up
docs/SPIKES.md          the branch that was measured and not merged
```

## What I would do differently at scale

Honest limitations, not a roadmap:

- **asyncio only.** There is no thread-safe or multi-event-loop version.
  Using the same decorated function from two event loops (two threads, say)
  is undefined — the registry is a plain `dict` with no lock, by design (see
  problem 3), and that design assumes one loop.
- **No cross-process coalescing.** Two workers behind a load balancer each
  run their own flight for the same key. A shared registry needs something
  like Redis and a very different consistency story than an in-process
  `dict`.
- **No metrics beyond `in_flight()`.** A production version would want a
  counter for flights started vs. calls made, to actually see the dedup
  ratio this buys, not just assert it in a benchmark.
- **The default key hashes the entire argument tuple.** Fine for scalars and
  small objects; a large argument makes hashing itself the expensive part of
  the call. `key=` exists for exactly this, but there's no guidance in the
  library for when to reach for it.

## License

MIT — see [LICENSE](LICENSE).

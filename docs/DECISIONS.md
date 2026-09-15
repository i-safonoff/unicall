# Decisions

Context, decision, cost. The cost column is the point: a decision recorded
without what it gave up is one nobody can revisit.

---

## 1. `asyncio.shield`, not a second cancellation-proof wrapper

**Context.** Multiple callers can be awaiting the same flight. One of them
cancelling — a timeout, a client disconnecting — must not take the flight
down for everyone else.

**Decision.** Every caller awaits the shared task through `asyncio.shield()`
rather than awaiting it directly.

**Cost.** A flight that every caller has abandoned still runs to completion;
nothing cancels it just because nobody is left waiting. That is deliberate,
not an oversight: cancelling a shared flight mid-way because the last waiter
walked away is fine for a read, and a correctness hazard for anything that
writes — a half-applied write restarted from zero can double the effect or
leave it half done. Running to completion is the safer default for a
decorator that doesn't know what the wrapped function does.

---

## 2. No lock around registering a flight

**Context.** "Check if a flight exists for this key, else start one" is a
classic check-and-set — the shape that needs a mutex under threads.

**Decision.** No lock. The check and the insert are synchronous, with no
`await` between them.

**Cost.** This only stays correct as long as nothing between the lookup and
the insert yields control — asyncio only switches coroutines at an `await`.
The failure mode this invites is a well-intentioned one: a future change adds
`await` there (a distributed lock, an audit log call) "for safety," and that
`await` is exactly the point where two callers can both decide no flight
exists and both start one. The comment in `_core.py` says so; nothing enforces
it beyond that.

---

## 3. The default key is the argument tuple itself, not a digest

**Context.** Turning `(args, kwargs)` into a dict key needs *something*
hashable.

**Decision.** Hash `(args, tuple(sorted(kwargs.items())))` directly, and
raise `UnhashableArgumentsError` immediately if that fails, instead of
falling back to something like `repr()` of the arguments.

**Cost.** A function called with a `list` or `dict` argument fails loudly
under the default key instead of silently getting a key that happens to work
today and breaks the moment two different objects `repr()` the same way (two
dicts with the same items in a different order, for instance). Callers with
unhashable arguments are expected to pass `key=` and choose what actually
identifies "the same call" themselves.

---

## 4. Eviction on completion, not on the next lookup

**Context.** A finished flight needs to leave the registry, or the registry
becomes a cache of every key ever seen instead of a set of keys currently in
flight.

**Decision.** `add_done_callback` removes the entry unconditionally when the
flight finishes. A key that's looked up once, ever, still gets cleaned up —
cleanup does not wait for someone to call that key again.

**Cost.** One extra scheduled callback per flight, and a small window (before
that callback runs) where a call landing in the same tick as completion can
still see the just-finished task and get its cached result immediately rather
than starting fresh. Harmless for a decorator whose whole point is "don't
run this twice concurrently" — the alternative (lazy cleanup on next lookup)
is the one with an actual leak.

---

## 5. No TTL, no caching past completion

**Context.** [`spike/ttl-cache`](../../tree/spike/ttl-cache) tried keeping a
finished result around for a short grace window, so a burst arriving just
after completion would reuse it instead of starting a new flight.

**Decision.** Not merged. See [SPIKES.md](SPIKES.md).

**Cost.** A burst that arrives a few milliseconds after the flight it wanted
already finished pays for a fresh one. What staying out buys: `@unicall()` on
a function means exactly one thing — no two concurrent calls duplicate work —
and never "the answer might be up to N milliseconds old." Mixing dedup with
staleness is a different decorator's job.

---

## 6. `Stats` has four counters and no latency histogram

**Context.** `Coalescer.stats()` needed *something* to say about how long a
flight takes, or a duration-free stats object is an odd thing to ship next to
a `metrics=` hook that receives every duration anyway.

**Decision.** No histogram, no percentiles, no min/max — just
`calls_total`, `flights_started`, `calls_coalesced`, `errors`.

**Cost.** `.stats()` can tell you the coalesce ratio and nothing about
latency. Doing that properly is buckets, decay, percentile estimation —
reimplementing a chunk of Prometheus's `Histogram` badly, in a library whose
entire pitch is doing one thing. `metrics=` already gets `duration` on every
`flight_finished` call; a real metrics system's `Histogram.observe(duration)`
is the right home for it.

---

## 7. `DistributedCoalescer` is a subclass with one overridden hook, not a parallel implementation

**Context.** Cross-process coordination needed to sit somewhere without
duplicating everything the local `Coalescer` already gets right — shield,
eviction, metrics, per-key isolation.

**Decision.** `Coalescer` gained one hook, `_execute()`, that by default
just calls the wrapped function. `DistributedCoalescer` overrides only that,
and inherits everything else unchanged.

**Cost.** The hook is `_`-prefixed and undocumented as public API — a
subclass reaching into it is relying on an implementation detail, not a
contract. Accepted because the alternative (a second class reimplementing
`__call__`, shield, and eviction) is the same logic maintained twice, and the
two copies drifting apart is a worse failure mode than a private hook.

---

## 8. Followers poll for a result; they don't subscribe to one

**Context.** A follower needs to learn when the leader's flight finishes.
Redis offers both a blocking poll (repeated `GET`) and Pub/Sub (subscribe,
get pushed to).

**Decision.** Poll, on a fixed interval (`poll_interval`, default 15ms).

**Cost.** A latency floor of up to one `poll_interval` on every follower,
and repeated `GET`s against Redis for the whole wait. What it buys: Pub/Sub
is fire-and-forget — a subscriber that subscribes after the publish already
happened just misses the message, a gap this project already hit once from
the other side, in [websocket-presence-board](https://github.com/i-safonoff/websocket-presence-board/blob/main/docs/DECISIONS.md#6-redis-pubsub-for-cross-instance-fan-out).
Making that gap safe needs a subscribe-then-check-then-listen dance that is
real complexity for a latency win a poll loop mostly gets anyway at these
intervals.

---

## 9. A leader's exception crosses as `RemoteFlightError`, never itself

**Context.** A follower needs to know the leader's flight failed, ideally
with enough information to act on it.

**Decision.** The leader publishes a type name and a message; a follower
raises `RemoteFlightError(type_name, message)` — never an instance of the
original exception class.

**Cost.** `except ValueError` in a follower's caller does not catch what was,
on the leader, a `ValueError`. Reconstructing the original type would mean
either guessing at its constructor (most exception subclasses take more than
a message) or deserializing something expressive enough to build arbitrary
objects — which is the same door pickle opens. `RemoteFlightError` is honest
about what actually crossed the wire: a string and a string.

---

## 10. The published result is checked before every acquire attempt, not only while waiting on someone else's flight

**Context.** [Found by a failing test](https://github.com/i-safonoff/unicall/commit/bbffbcd):
a fast (or instant, or immediately-failing) function lets its leader
acquire, run, publish, and release the lock before a second caller's own
`try_acquire` has even reached Redis. That caller finds no lock, concludes
it should lead too, and runs the function a second time.

**Decision.** `_execute` checks for an existing published result before
every `try_acquire`, not only inside the follower's wait loop.

**Cost.** One extra Redis round trip (`GET`) on the leader's own path too,
paid on every acquire attempt including the very first one, for a race that
only bites fast functions. Worth it: the alternative is a distributed
coalescer that silently stops coalescing for exactly the calls cheap enough
that duplicating them looks harmless — until it isn't.

---

## 11. A process that loses its lease publishes nothing

**Context.** Renewal can fail mid-run (another process's lease has since
expired ours and it took over). That process's own computation is still
running and will still finish.

**Decision.** If renewal ever fails, the rest of that run publishes no
result, no error, and releases nothing — it just returns the answer to its
own local callers, the same as if there were no distributed backend at all.

**Cost.** A process in this state does real work that helps no one else,
and the exactly-once guarantee this backend provides is closer to
*at-most-duplicated-rarely* than a hard guarantee — inherent to lease-based
coordination without a fencing token on the resource being protected, which
this library has no way to require. Local correctness — the value returned
to *this* process's own callers — never depended on any of this in the first
place, so it's unaffected either way.

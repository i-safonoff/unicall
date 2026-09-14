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

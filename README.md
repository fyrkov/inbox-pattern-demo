# The Inbox Pattern

The inbox pattern is the consumer-side mirror of the outbox pattern. Where the outbox guarantees that a service reliably
*publishes* events, the inbox guarantees that a service reliably *processes* the events it receives - exactly once, in
its own time, and on its own terms.

## What it is

When a message arrives, the consumer does **not** process it immediately. It first **persists the raw event into a
local `inbox` table**, then acknowledges the broker. A separate step (usually a background worker) reads from that table
and does the real work.

So there are two decoupled phases:

1. **Ingest** - capture and store an event, do nothing else.
2. **Process** - process the stored event, independently.


## How it works

A typical inbox table:

```sql
create table inbox (
  event_id     text primary key,                   -- idempotency key
  payload      jsonb not null,                     -- the event, stored on receipt
  status       text not null default 'pending',    -- pending | done | failed
  received_at  timestamptz not null default now(),
  processed_at timestamptz,
  error        text
);
```

**Ingest** - consume from the broker, insert, commit the offset:

```sql
insert into inbox (event_id, payload) values (:id, :payload)
on conflict (event_id) do nothing;
```

The `on conflict do nothing` is the deduplication: if the same event is delivered twice, the second insert is a no-op. This step never runs business logic, so it should never stuck -  it just captures the event.

**Process** - a worker picks up pending rows and acts:

```sql
select * from inbox where status = 'pending' order by received_at limit 100;
```

This ofc course requires an index like:
```
craete index idx on inbox (received_at) where status = 'pending';
```

On success the worker sets `status = 'done'`. On failure it sets `status = 'failed'` and records the
`error`. In both cases it also sets the `processed_at`. 

❗ Processing the event and updating its status must happen in the **same transaction** as any business writes, so
state and status can never disagree.

## Replay 
Reprocessing a failed event is one simple statement in your own database:

```sql
update inbox set status = 'pending', error = null where event_id = '...';
```

The worker re-picks it on its next pass. No broker, no offset reset, no infrastructure ticket.

## Pros

- **Reliable, exactly-once processing.** The dedup key takes care of duplicates; the same-transaction status update
  kills the "processed but crashed before acking" gap.
- **Self-service replay.** Reprocess any event by flipping a column - the ergonomics of the classic polling outbox, on
  the consumer side.
- **Decoupled ingestion from processing.** A slow or failing downstream never backs up your broker consumption. You
  ingest fast, process at your own pace, retry on your own schedule.
- **Independent of broker retention.** Because you own the payload, you can replay events long after they've aged out of
  the topic.
- **Free audit and error log.** Every event you ever received, with its status and failure reason, sitting in a
  queryable table.
- **Natural backpressure and batching.** The worker controls throughput; you can batch, throttle, or pause processing
  without touching the consumer.

## Cons

- **You store every payload.** The inbox table is ;arger comparing to storing only dedup keys and grows with the event volume. You need a retention/archival policy for it.
- **More moving parts.** A table, an ingest path, and a separate worker, versus "just handle the message in the consumer
  callback." Also the worker needs monitoring, scaling, and its own failure handling.

## Gotchas

- **Status updates must be transactional with business writes.** If you process the event, write to your domain tables,
  and update `status = done` in separate transactions, a crash in between leaves you in a lying state. Keep them in one
  transaction.
- **Idempotency still matters at the process step, not just ingest.** A worker can crash mid-processing and retry the
  same `pending` row. The business logic itself must be safe to run twice, or be guarded by the same transaction that
  flips the status.
- **The inbox table needs pruning.** It is append-heavy and grows forever if you let it. Archive or delete old `done`
  rows, or it becomes your largest, slowest table. Watch for bloat from high-churn updates.
- **Worker polling can be wasteful or laggy.** Naive `select ... where status = 'pending'` polling either hammers the DB
  or adds latency. Use `LISTEN/NOTIFY`, `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent workers, or a sensible
  poll interval.
- **Ordering is not free.** If processing order matters, the worker must respect it (order by receipt, or partition by
  key). A naive parallel worker pool can reorder events.


## When to use it

Reach for the inbox when:

- **Processing is heavy, slow, or failure-prone** (calls external systems, long computations) and you don't want that
  coupled to broker consumption.
- **You need self-service, fine-grained replay** of individual events without touching shared broker infrastructure.


## The one-line summary

The inbox pattern trades a bit of storage and a background worker for **reliable, exactly-once, replayable processing
that you fully control from your own database** - the consumer-side counterpart to the outbox, and the antidote to "to
reprocess one event, please file a ticket with the Kafka team."

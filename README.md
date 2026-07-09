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
- **Self-service replay.** Reprocess any event by updating a column in local DB. No dependency on the Kafka broker offset reset operation.
- **Decoupled ingestion from processing.** A slow or failing downstream never backs up your broker consumption. You
  ingest fast, process at your own pace, retry on your own schedule.
- **Independent of broker retention.** Because you own the payload, you can replay events long after they've aged out of
  the topic.
- **Free audit and error log.** Every event you ever received, with its status and failure reason, sitting in a
  queryable table.
- **Natural backpressure and batching.** The worker controls throughput; you can batch, throttle, or pause processing
  without touching the consumer.

## Cons

- **More storage.** The inbox table contains payloads and therefore is larger comparing to storing only dedup keys. It grows with the event volume and needs a partitioning/retention/archival policy.
- **More moving parts.** A table, an ingest path, and a separate worker, versus "just handle the message in the consumer
  callback." Also the worker needs monitoring, scaling, and its own failure handling.

## Other notes

- **Ordering + Parallelization is not free.** Unlike the built-in ordering guarantee within partitioned Kafka topics, a local pool of parallel workers can reorder events. Workers must keep per-entity order intact while still running in parallel.


## When to use it

Think of the inbox when:

- **You need self-service, fine-grained replay** of individual events without touching shared broker infrastructure.
- **Processing is heavy, slow, or failure-prone** (calls external systems, long computations) and you don't want that
  coupled to broker consumption.


## The one-line summary

The inbox pattern trades a bit of storage and a background worker for **reliable, exactly-once, replayable processing
that you fully control from your own database** - the consumer-side counterpart to the outbox, and the antidote to "to
reprocess one event, please file a ticket with the Kafka team."

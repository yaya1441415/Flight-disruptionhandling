# Flight Disruption Handling

An event-driven pipeline for handling flight disruptions (delays, cancellations) that
calls an unreliable third-party rebooking API — built to explore how three resilience
patterns interact under real AWS infrastructure: **EventBridge routing**, a **circuit
breaker with externalized state**, and **idempotent event processing**.

## Why these three patterns, together

They aren't three independent features — each one exists because of a problem the
other two create:

- **EventBridge guarantees at-least-once delivery.** That's the contract, not a bug —
  the same `FlightDisrupted` event can legitimately arrive twice.
- **The rebooking API is unreliable.** Without a breaker, every retry (and every
  duplicate) piles onto a struggling dependency, burning Lambda concurrency on timeouts.
- **A breaker that trips causes *more* retries** — a failed invocation gets redelivered,
  which makes duplicate delivery worse, not better.

So idempotency isn't optional once you have a breaker, and the breaker isn't optional
once you have a flaky dependency you don't control.

## Architecture

```
POST /disruptions
        │
        ▼
 Producer Lambda ──PutEvents──▶ EventBridge Bus (FlightDisruptionBus)
                                          │
                                 rule: detail-type=FlightDisrupted
                                          │
                                          ▼
                                 Rebooking Lambda
                                 ┌──────────────────────────────┐
                                 │ ① claim event ID              │──▶ IdempotencyTable (DynamoDB)
                                 │    already COMPLETE? → cached │
                                 │    result, stop here          │
                                 ├──────────────────────────────┤
                                 │ ② check circuit breaker state │──▶ BreakerStateTable (DynamoDB)
                                 │    OPEN & cooling? → 503, stop│
                                 ├──────────────────────────────┤
                                 │ ③ call the rebooking API      │──▶ Fake Rebooking API
                                 │    record success/failure     │    (Function URL,
                                 └──────────────────────────────┘     FAILURE_RATE env var)
```

A request only reaches step ③ if it clears both gates. Either gate can end the request
early — that branching is the actual point of the project, not the individual AWS
services.

## Components

| Component | Purpose |
|---|---|
| **Producer Lambda** (Function URL) | Accepts a disruption report over HTTP, publishes it as a `FlightDisrupted` event. |
| **EventBridge Bus + Rule** | Routes `FlightDisrupted` events to the Rebooking Lambda. |
| **Rebooking Lambda** | Enforces idempotency, then the circuit breaker, then calls the rebooking API. |
| **BreakerStateTable** (DynamoDB) | Holds breaker state (`CLOSED`/`OPEN`/`HALF_OPEN`), failure count, trip timestamp — externalized since Lambda has no memory between invocations. |
| **IdempotencyTable** (DynamoDB) | Tracks which event IDs have been claimed/completed, with a stale-claim timeout so a crashed invocation doesn't permanently block a retry. |
| **Fake Rebooking API** (Function URL) | A stand-in third-party dependency with a `FAILURE_RATE` env var, so failure can be forced on demand instead of waited for. |

## What makes the breaker and idempotency logic non-trivial

Both pieces have to survive genuine concurrency, not just sequential requests, because
Lambda invocations can run at the exact same instant with no shared memory between them.

**The breaker's failure counter is an atomic `ADD`, not a read-then-write.** Two
concurrent invocations both doing `count = read(); write(count + 1)` in application code
can silently lose one of the two updates — DynamoDB's `ADD` moves the arithmetic into
the database itself, so it's a single indivisible operation no matter how many requests
hit it at once:

```python
resp = table.update_item(
    Key={"pk": "breaker#fake-rebooking-api"},
    UpdateExpression="ADD failure_count :incr",
    ExpressionAttributeValues={":incr": 1},
    ReturnValues="UPDATED_NEW",
)
if resp["Attributes"]["failure_count"] >= FAILURE_THRESHOLD:
    ...  # trip to OPEN
```

**The half-open recovery probe is a conditional write race.** When the cooldown
elapses, every waiting invocation tries the same write; DynamoDB guarantees only one of
them actually succeeds, and everyone else gets a catchable, immediate failure instead of
silently overwriting each other:

```python
try:
    table.update_item(
        Key={"pk": "breaker#fake-rebooking-api"},
        UpdateExpression="SET #s = :half",
        ConditionExpression="#s = :open",   # only succeeds if still OPEN
        ExpressionAttributeNames={"#s": "state"},
        ExpressionAttributeValues={":half": "HALF_OPEN", ":open": "OPEN"},
    )
    won_the_claim = True          # this invocation makes the one real trial call
except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        won_the_claim = False     # someone else already claimed it — fail fast
    else:
        raise
```

**The idempotency claim uses the same primitive**, keyed on the event's own `id`
instead of breaker state:

```python
try:
    idempotency_table.put_item(
        Item={"pk": event_id, "status": "PROCESSING", "claimed_at": int(time.time())},
        ConditionExpression="attribute_not_exists(pk)",
    )
    claimed = True
except ClientError as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        claimed = False   # this event is already claimed — check its status next
    else:
        raise
```

If the claim fails, a plain boolean lock would dead-end here forever if the original
claimant crashed. Instead, a stale `PROCESSING` record can be reclaimed — again via a
conditional write, this time keyed on the exact old timestamp so two simultaneous
reclaim attempts can't both win:

```python
is_stale = time.time() - float(existing_item["claimed_at"]) >= PROCESSING_TIMEOUT_SECONDS
if not is_stale:
    raise Exception("still processing, retry later")   # let it be retried later, don't redo the work

idempotency_table.update_item(
    Key={"pk": event_id},
    UpdateExpression="SET claimed_at = :new_claimed_at",
    ConditionExpression="claimed_at = :old_claimed_at",  # only succeeds if nobody else already reclaimed it
    ExpressionAttributeValues={
        ":new_claimed_at": int(time.time()),
        ":old_claimed_at": existing_item["claimed_at"],
    },
)
```

## Environment variables

| Lambda | Variable | Purpose |
|---|---|---|
| Producer | `EVENT_BUS_NAME` | The EventBridge bus to publish `FlightDisrupted` events onto. |
| Fake Rebooking API | `FAILURE_RATE` | `0`–`1`, probability the API simulates a failure — dial to `1` to force a breaker trip on demand. |
| Rebooking Lambda | `TABLE_NAME` | Breaker state table name. |
| Rebooking Lambda | `FAKE_API_URL` | Function URL of the fake rebooking API. |
| Rebooking Lambda | `COOLDOWN_SECONDS` | How long the breaker stays `OPEN` before allowing one half-open probe. |
| Rebooking Lambda | `FAILURE_THRESHOLD` | Consecutive failures (in `CLOSED` state) before the breaker trips to `OPEN`. |
| Rebooking Lambda | `IDEMPOTENCY_TABLE_NAME` | Idempotency table name. |
| Rebooking Lambda | `PROCESSING_TIMEOUT_SECONDS` | How long a `PROCESSING` claim is trusted before it's treated as abandoned and reclaimed. |

All of these are wired as CDK stack outputs of one resource into another Lambda's
`environment` (e.g. `table.table_name`, `fake_rebooking_url.url`) — never hardcoded, so
tearing down and redeploying the stack never leaves a stale reference behind.

## Getting started

Requires Python 3.12, the AWS CDK CLI, and AWS credentials configured locally.

```bash
python -m venv .venv
.venv\Scripts\activate      # or: source .venv/bin/activate
pip install -r requirements.txt
cdk deploy
```

`cdk deploy` prints the Producer Lambda's URL and the Rebooking Lambda's function name
as stack outputs.

## Testing the pipeline

**Trigger it end-to-end** (producer → EventBridge → Rebooking Lambda):

```powershell
Invoke-RestMethod -Uri "<ProducerLambdaUrl>" -Method POST -Body '{"flightNumber":"AA123","delayMinutes":200}' -ContentType "application/json"
```

**Trip the breaker deterministically** — set `FAILURE_RATE` to `"1"` on the fake API in
the stack, redeploy, then invoke the Rebooking Lambda directly a few times and watch the
response body shift from `"rebooking failed"` (real calls) to `"circuit open"`
(fail-fast, no call made).

**Prove idempotency** — invoke the Rebooking Lambda directly with the same event `id` in
the payload twice; the second call returns the cached result instead of reprocessing.

**Tear it all down when you're done** — nothing here is always-on:

```bash
cdk destroy
```

## Not included (deliberately out of scope for now)

Scoped as a weekend project first: one producer, one routing rule, one consumer with
idempotency and a breaker. Layered on top later, if pursued: a DLQ and redrive script
for events that exhaust retries, correlation IDs and structured logging for tracing one
disruption's journey across Lambdas in CloudWatch Logs Insights, additional
content-based routing rules and a schema registry, and — as a stretch — a saga pattern
for compensating a successful rebooking against a failed downstream step.

## Stack

Python · AWS CDK · Lambda · EventBridge · DynamoDB

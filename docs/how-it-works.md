# How it works

What the processor does between a trigger and a FlowFile, and what it keeps so that it can carry on.

## The loop

One trigger does one pass:

1. If a wait is still running after a failure, return at once.
2. If an initial snapshot is not finished, write one batch of it and return.
3. Open the change stream cursor, or keep the one already open.
4. Take events from the cursor until the batch is full, the batch time is up, or the stream has nothing
   more for now.
5. Write the events as records into one FlowFile, transfer it, store the resume token of the last event in
   the same transaction, and commit.

The cursor lives in a field and stays open between triggers. It is closed and opened again only when
something goes wrong, when an invalidate ends it, or when the processor stops.

## Where a stream starts

The stored resume token always wins. `Start Position` only decides what happens when there is none:

| Start Position | Without a stored token |
|---|---|
| `Now` | the stream starts at the moment the processor starts; changes made before that are not captured |
| `Timestamp` | the stream starts at the given second, as far back as the oplog reaches |
| `Initial Snapshot` | the documents already in the collection are written first, then the stream carries on from the moment the snapshot was taken |

The stream is always opened with `startAfter`, never with `resumeAfter`. The two do the same for an
ordinary token, but only `startAfter` accepts the token of an invalidate event, which is what lets the
stream survive a dropped collection.

## What is kept in state

State is cluster wide, so the position survives a restart and a change of the primary node.

| Key | Meaning |
|---|---|
| `resume.token` | the position of the last change event written to a committed FlowFile |
| `stream.source` | what that token belongs to, for example `collection:lab.orders` |
| `snapshot.start.time` | the moment the initial snapshot is consistent with |
| `snapshot.done` | set once the snapshot has read the whole collection |
| `snapshot.last.id` | the identifier of the last document the snapshot wrote, while it is still running |

`stream.source` exists because a resume token means nothing outside the scope it came from. If the scope,
the database or the collection changes while a token is stored, the processor refuses to start and says
which scope the stored position belongs to. Clearing the state is then a decision, not an accident. The
pipeline is not part of it: a token is a position, the server accepts it whatever the pipeline is, so a
changed filter simply applies from the stored position on.

## Why nothing is lost

The resume token is written with `session.setState` **before** the session is committed, so the token and
the FlowFiles of the batch belong to the same transaction. Either both are there or neither is. The stored
position can never be ahead of what was delivered.

When anything fails in the middle of a batch, the batch is rolled back, the cursor is closed, and the next
trigger opens a new one from the last committed token. The events of the failed batch are read again.

Before the first commit there is no committed token, so the position the cursor reported when it was opened
is kept in memory and used instead. Without that, a batch that failed before the first commit would reopen
the stream at "now" and step over exactly the events that failed.

The consequence is at-least-once: an event can arrive twice, never zero times. See the README for what to
deduplicate on.

## An idle stream still moves

A collection that changes rarely would otherwise keep an old position while the oplog moves on. The server
reports how far it has read even when it sends no event, so a trigger that finds nothing stores that
position. A quiet collection therefore stays close to the end of the oplog rather than falling out of it.

## Invalidate

When the watched collection is dropped or renamed, the server sends a `drop` event and then an
`invalidate`, and ends the stream. Both are written like any other event. The invalidate token is committed
and the cursor is closed, and the next trigger opens a new stream with `startAfter` on that token. If the
collection is created again, its changes are captured without anything being cleared by hand.

With database scope a dropped collection is an ordinary event; the stream is not ended.

## When the position is lost

The server answers in two different ways when it can no longer serve a stored position:

- `ChangeStreamHistoryLost` (code 286), when the oplog no longer reaches back that far,
- `ChangeStreamFatalError` (code 280) with *"cannot resume stream; the resume token was not found"*, which
  is what a server actually answers when the token itself is not in the oplog.

Both count as a lost position. Any other fatal stream error stays an ordinary failure and is retried.

`On History Lost` decides what happens next:

- `Fail`, the default, keeps the stored position and reports an error. The flow makes no progress until
  somebody decides what to do, which is the point: the changes in the gap cannot be read from this server,
  and that should not pass unnoticed.
- `Restart From Now` clears the stored position and continues with the changes made from then on. The gap
  is given up on purpose and a warning records it.

## Failures and waiting

After a failed attempt the processor waits before trying again: one second, then two, four, and so on up to
a minute. The wait is given up as soon as a trigger succeeds. The first failure is logged as an error and
repeats are logged at a limited rate, so an outage does not flood the bulletin board.

MongoDB has no socket timeout by default, so a server that accepts a connection and then stops answering
would hold a trigger open. Set the timeouts on the client service; the README shows the parameters.

## The initial snapshot and the handover

The snapshot exists so that a new flow can start with what is already in the collection and then continue
with the changes, without a gap in between.

1. Before the first document is read, the processor asks the server for its current time and stores it as
   `snapshot.start.time`.
2. The documents are read in the order of their identifiers, `Snapshot Batch Size` at a time, and written
   as records with the operation `read`. They are not changes, so they carry no resume token and no wall
   clock time; their cluster time is the moment the snapshot is consistent with.
3. Each batch is committed together with the identifier it reached, so a processor stopped in the middle
   carries on from there instead of reading the collection again.
4. When a batch comes back shorter than the batch size, the collection is exhausted and `snapshot.done` is
   set in the same commit.
5. The change stream is then opened at `snapshot.start.time`.

A change made while the snapshot runs is therefore either already in the documents it read, or it arrives
afterwards as an event, or both. The last write before the snapshot started also arrives again as an event,
because a stream opened at a timestamp includes the operations at that timestamp. That repeat is the
at-least-once guarantee, not a defect.

The snapshot reads one collection, so it cannot be combined with database scope; that combination is
refused while the processor is being configured.

## What the processor never does

It never writes to the source. It needs `changeStream` and `find` on the watched scope and nothing else: no
collection is created, no index is added, no `collMod` is run. Pre-images are turned on by an
administrator, not by the processor, and *Verify* reports it when they are asked for and missing.

An integration test reads the server's `opcounters` before and after a run and fails if the number of
inserts, updates or deletes moved.

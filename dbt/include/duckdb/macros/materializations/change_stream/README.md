# DuckDB Model Change Stream (CDC)

Local DuckDB feature that lets downstream incremental models consume the
**actual row changes** of an ordinary model table. A rebuild or a failed run
cannot make a consumer miss or double-count a change.

It adds two materializations and one read macro on top of the existing DuckDB
adapter. The ordinary `table` materialization and plain `ref()` are untouched,
so existing projects behave exactly as before.

## Concepts

| Object | Purpose |
| --- | --- |
| **change_stream** model | A model materialized as a normal business table that *also* publishes the row-level diff of every rebuild to an append-only change log. |
| **stream_consumer** model | A downstream model that reads a change log from its own committed progress up to a fixed upper bound. |
| `stream_changes(source)` | Macro returning the bounded change relation a consumer reads in its SQL. |

For each change_stream source, three control tables and a sequence are created
in a separate change schema (`_dbt_changes` by default):

* `<model>__changes` — one row per row change, with a monotonic `seq`,
  `batch_id`, `change_type` (`insert` / `update` / `delete`), the business
  `key`, and JSON `before_image` / `after_image`.
* `<model>__batches` — one row per publication (even an empty change set), with
  insert/update/delete counts.
* `<model>__progress` — one row per consumer model: its last committed `seq`.
* `<model>__seq` — the per-source sequence that orders changes.

## Publishing changes

```sql
{{ config(
    materialized='change_stream',
    business_key='id'            -- string or list of columns
) }}

select id, name, amount from {{ ref('raw_customers') }}
```

Every run rebuilds the table and, in the **same transaction**, compares the old
and new tables on `business_key`:

* rows with a key only in the new table → `insert`
* rows with a key only in the old table → `delete` (an empty new table therefore
  publishes deletes for every old row, plus a batch row)
* rows present on both sides whose shared columns differ → `update`

The business-table swap and the change batch commit atomically. If anything
fails before commit, both roll back together, so a batch is never published
without its table and vice versa.

### Change row shape

| Column | Type | Notes |
| --- | --- | --- |
| `seq` | bigint | Globally ordered within the source. |
| `batch_id` | bigint | The publication this change belongs to. |
| `change_type` | varchar | `insert`, `update`, or `delete`. |
| `key` | json | The business key, e.g. `{"id":7}`. |
| `before_image` | json | Full old row; `null` for inserts. |
| `after_image` | json | Full new row; `null` for deletes. |
| `changed_at` | timestamp | Publication time. |

Images are JSON so the model's schema can evolve between runs without altering
the log. Read a field back with a typed cast, e.g.
`after_image.amount::decimal(12,2)` or `(after_image ->> 'name')::varchar`.

Update detection compares only the columns present in **both** the old and new
table. Adding or dropping a column therefore does not turn otherwise unchanged
rows into updates; the new column simply appears in subsequent images.

## Consuming changes

```sql
{{ config(
    materialized='stream_consumer',
    stream_of='customers'        -- node name of the change_stream model
) }}

select
    seq,
    change_type,
    key.id::integer as id,
    after_image,
    before_image
from {{ stream_changes('customers') }}
```

`stream_changes()` returns exactly the changes in the half-open window
`(last_seq, upper_seq]`:

* `last_seq` is this consumer's highest committed seq (0 before its first run).
* `upper_seq` is a **fixed** bound captured once for the dbt invocation. The
  bound is cached per (source, consumer), so every reference inside the model
  sees the same window even if the publisher commits while the model builds —
  repeated reads cannot drift across batches.

The consumer's progress cursor is advanced to `upper_seq` **inside the build
transaction, after the in-transaction post hooks have passed**. A failure rolls
back the model table and the cursor together, so the next run replays the same
window exactly once. Re-running with no new changes is a no-op that does not
rebuild the table or move the cursor.

Each consumer has its own row in `__progress`, so independent consumers advance
at their own pace and never interfere with each other.

### Consumer modes

* `consumer_mode='replace'` (default) — rebuild the whole model from the bounded
  window each run.
* `consumer_mode='append'` — append each new window's rows to the existing table
  (the table is created normally on the first run).

### Referencing the source

`stream_of` and `stream_changes()` accept the change_stream model's **node name
string** (preferred, so the parsed manifest stays serializable). A relation from
`ref()` also works inside `stream_changes()`, and a fully-qualified
`'database.schema.table'` string is accepted for sources outside the project.

## Configuration

| Config | Where | Default | Meaning |
| --- | --- | --- | --- |
| `business_key` | change_stream | required | Column name or list of column names identifying a business row. |
| `stream_of` | stream_consumer | required | Node name (or qualified string) of the change_stream source. |
| `consumer_mode` | stream_consumer | `replace` | `replace` or `append`. |
| `change_schema` | either | `_dbt_changes` | Schema holding the control tables. A consumer reading a publisher with a custom `change_schema` must set the same value; a mismatch raises an actionable compilation error. |

The default schema can also be set project-wide with the variable
`duckdb_change_stream_schema`.

## Guarantees and boundaries

* **No missed or duplicated changes** — consumers resume from their committed
  seq; failures replay the identical window and success advances exactly once.
* **Atomic publication** — table and change batch share one transaction.
* **Deletes on empty results** — an empty model still emits delete rows and a
  batch record.
* **History is retained** — the change log is append-only; a slow consumer does
  not block the publisher from continuing to publish.
* **Plain refs are unaffected** — `ref()` of a change_stream model returns the
  normal business table; all bookkeeping lives in the separate change schema.
* **Single NULL-key rows are supported.** Because row presence is tracked with a
  non-null sentinel (rather than by testing key columns for NULL), a row whose
  key is NULL still keeps its identity across rebuilds: changing a non-key column
  emits an `update`, and removing the row emits a `delete`.
* **Keys must be unique; duplicate keys (including multiple NULL keys) are not
  supported.** Row identity cannot be established when two rows share a key, so a
  many-to-many outer join may pair them arbitrarily and misclassify their
  changes. Declare a key that is unique in practice; it may contain NULL only
  when at most one row has a NULL key.

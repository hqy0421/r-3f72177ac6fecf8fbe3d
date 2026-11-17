"""Change-stream (CDC) support for the local DuckDB adapter.

This mixin backs the ``change_stream`` publisher materialization and the
``stream_consumer`` materialization. The publisher writes a per-source,
append-only change log plus batch metadata; each consumer tracks its own
progress through that log.

The read window for a consumer is the half-open interval
``(last_seq, upper_seq]`` where:

* ``last_seq``  is the highest sequence the consumer has successfully
  committed (0 before its first run), and
* ``upper_seq`` is a *fixed* upper bound captured once per model run.

Capping the upper bound at run start is what makes repeated reads inside one
model (the compiled query may reference the stream more than once) see one
stable, identical batch, and stops a publisher that commits midway through the
consumer's build from leaking a later batch into this run. The bound is cached
per (source, consumer) for the life of the dbt invocation.

All schema/relation naming is resolved in Jinja (which owns the configured
``change_schema``) and passed in as rendered relations, so this module only
deals with already-qualified relation objects.
"""

from typing import Tuple

from dbt.adapters.base.meta import AdapterMeta
from dbt.adapters.base.meta import available
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("DuckDB")


class ChangeStreamAdapterMixin(metaclass=AdapterMeta):
    """CDC cursor/upper-bound helpers.

    Using ``AdapterMeta`` as the metaclass builds the ``_available_`` set on the
    mixin itself, so ``DuckDBAdapter`` (whose own metaclass merges each base's
    ``_available_``) exposes the ``@available`` methods to Jinja at both parse
    time and runtime. A plain mixin would leave them unregistered.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Per-invocation cache of the fixed upper bound (max published seq)
        # captured for each (source_id, consumer) pair. Different consumers
        # never share a cursor, and repeated reads within one model cannot drift
        # across batches.
        self._change_stream_upper_bounds: dict[Tuple[str, str], int] = {}

    # -- progress and bounds -------------------------------------------------

    def _change_stream_table_exists(self, relation) -> bool:
        """Lightweight existence check for a change-stream control table.

        Consumers may be parsed/built before their publisher has ever run, in
        which case the progress table does not exist yet. Treating a missing
        table as "no progress" lets the first run start at seq 0.
        """
        sql = f"""
            select 1 from information_schema.tables
            where lower(table_catalog) = lower('{relation.database}')
              and lower(table_schema) = lower('{relation.schema}')
              and lower(table_name) = lower('{relation.identifier}')
        """
        try:
            _, cursor = self.connections.add_select_query(sql)
            return len(cursor.fetchall()) > 0
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(f"change-stream existence check failed: {exc}")
            return False

    @available
    def change_stream_relation_exists(self, relation) -> bool:
        """Whether a specific change-stream control relation exists."""
        return self._change_stream_table_exists(relation)

    @available
    def change_stream_find_control_schema(self, database: str, table_name: str):
        """Return the schema hosting a change control table by name, or None.

        Used to diagnose a consumer whose configured ``change_schema`` does not
        match its publisher's: if the table exists in another schema we can
        point the user at the correct value instead of silently reading nothing.
        """
        sql = f"""
            select table_schema from information_schema.tables
            where lower(table_catalog) = lower('{self._escape(database)}')
              and lower(table_name) = lower('{self._escape(table_name)}')
            limit 1
        """
        try:
            _, cursor = self.connections.add_select_query(sql)
            rows = cursor.fetchall()
            return rows[0][0] if rows else None
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(f"change-stream schema lookup failed: {exc}")
            return None

    @staticmethod
    def _change_stream_cache_key(source_id: str, consumer: str) -> Tuple[str, str]:
        return (source_id, consumer)

    @available
    def change_stream_last_seq(self, progress_relation, consumer: str) -> int:
        """Return the highest seq this consumer has committed, or 0."""
        if not self._change_stream_table_exists(progress_relation):
            return 0
        sql = (
            f"select last_seq from {progress_relation.render()} "
            f"where consumer = '{self._escape(consumer)}'"
        )
        _, cursor = self.connections.add_select_query(sql)
        rows = cursor.fetchall()
        return int(rows[0][0]) if rows else 0

    @available
    def change_stream_upper_seq(
        self, changes_relation, source_id: str, consumer: str
    ) -> int:
        """Return the fixed upper bound for this (source, consumer) run.

        The first call in an invocation queries the current max published seq and
        caches it; every later call (including repeated references inside one
        compiled query) returns the cached value, so a model always reads one
        immutable window even if the publisher commits while it builds.
        """
        key = self._change_stream_cache_key(source_id, consumer)
        cached = self._change_stream_upper_bounds.get(key)
        if cached is not None:
            return cached
        if not self._change_stream_table_exists(changes_relation):
            upper = 0
        else:
            sql = f"select coalesce(max(seq), 0) from {changes_relation.render()}"
            _, cursor = self.connections.add_select_query(sql)
            upper = int(cursor.fetchall()[0][0])
        self._change_stream_upper_bounds[key] = upper
        return upper

    @available
    def change_stream_read_window(
        self, changes_relation, progress_relation, source_id: str, consumer: str
    ) -> Tuple[int, int]:
        """Return the (last_seq, upper_seq] window for this consumer run."""
        last_seq = self.change_stream_last_seq(progress_relation, consumer)
        upper_seq = self.change_stream_upper_seq(changes_relation, source_id, consumer)
        return last_seq, upper_seq

    @available
    def change_stream_advance_progress(
        self, progress_relation, consumer: str, upper_seq: int, changes_relation
    ) -> None:
        """Upsert the consumer's progress to the run's fixed upper bound.

        Runs inside the consumer's build transaction, after its in-transaction
        post hooks have passed. A failure/rollback undoes this upsert together
        with the model table, leaving the cursor at the previous seq so the next
        run replays the same window.
        """
        batch_sql = (
            f"select coalesce(max(batch_id), 0) from {changes_relation.render()} "
            f"where seq <= {int(upper_seq)}"
        )
        sql = f"""
            insert into {progress_relation.render()}
              (consumer, last_seq, last_batch, updated_at)
            values ('{self._escape(consumer)}', {int(upper_seq)}, ({batch_sql}), now())
            on conflict (consumer) do update
              set last_seq = excluded.last_seq,
                  last_batch = excluded.last_batch,
                  updated_at = excluded.updated_at
        """
        self.execute(sql, auto_begin=False, fetch=False)

    @staticmethod
    def _escape(value: str) -> str:
        return str(value).replace("'", "''")

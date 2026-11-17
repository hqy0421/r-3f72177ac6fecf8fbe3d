"""Functional tests for the local DuckDB model change stream (CDC).

Covers the publisher (`change_stream`) and the downstream consumers
(`stream_consumer` + `stream_changes()`):

* insert/update/delete detection on a declared business key, with JSON
  before/after images
* an empty new table still publishes delete rows and a batch row
* business table and change batch commit atomically (a failing in-transaction
  post hook rolls both back)
* each consumer keeps an independent progress cursor and only advances after a
  successful, hook-passing transaction; a failure replays the same window
* a fixed upper bound means repeated reads within a model cannot drift across
  batches, and re-running with no new changes is a no-op
* ordinary `ref()` still sees the plain business table
"""

import pytest

from dbt.tests.util import run_dbt
from dbt.tests.util import write_file


PUBLISHER = """
{{ config(materialized='change_stream', business_key='id') }}
select id, "name", amt from {{ ref('source_rows') }}
"""

CONSUMER_REPLACE = """
{{ config(materialized='stream_consumer', stream_of='pub') }}
select seq, batch_id, change_type, key.id::integer as id,
       before_image, after_image
from {{ stream_changes('pub') }}
"""

CONSUMER_APPEND = """
{{ config(materialized='stream_consumer', stream_of='pub', consumer_mode='append') }}
select seq, change_type, key.id::integer as id,
       coalesce((after_image ->> 'amt')::integer, 0) as amt
from {{ stream_changes('pub') }}
"""

PLAIN_REF = """
{{ config(materialized='table') }}
select id, amt from {{ ref('pub') }}
"""

DOUBLE_READ = """
{{ config(materialized='stream_consumer', stream_of='pub') }}
select 'a' as which, count(*) as n, max(seq) as hi
from {{ stream_changes('pub') }}
union all
select 'b', count(*), max(seq)
from {{ stream_changes('pub') }}
"""

CONSUMER_REPLACE_FAILING_HOOK = """
{{ config(
    materialized='stream_consumer',
    stream_of='pub',
    post_hook="select (select nope from no_such_table)"
) }}
select seq, batch_id, change_type, key.id::integer as id,
       before_image, after_image
from {{ stream_changes('pub') }}
"""

SOURCE_ROWS_INITIAL = """
select 1 as id, 'a' as "name", 10 as amt
union all select 2, 'b', 20
"""

SOURCE_ROWS_ROUND2 = """
select 1 as id, 'a2' as "name", 11 as amt
union all select 3, 'c', 30
"""

SOURCE_ROWS_EMPTY = """
select 1 as id, 'x' as "name", 1 as amt where false
"""

CHANGE_SCHEMA = "_dbt_changes"


class TestChangeStream:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_rows.sql": "{{ config(materialized='table') }}\n" + SOURCE_ROWS_INITIAL,
            "pub.sql": PUBLISHER,
            "consumer_replace.sql": CONSUMER_REPLACE,
            "consumer_append.sql": CONSUMER_APPEND,
            "plain_ref.sql": PLAIN_REF,
            "double_read.sql": DOUBLE_READ,
        }

    # -- fully qualified name helpers (test schema is per-class) --
    def _t(self, project, name):
        return f'''"{project.database}"."{project.test_schema}"."{name}"'''

    def _c(self, project, suffix):
        return f'''"{project.database}"."{CHANGE_SCHEMA}"."pub__{suffix}"'''

    def _changes(self, project):
        return project.run_sql(
            f"select change_type, key, before_image, after_image "
            f"from {self._c(project, 'changes')} order by seq",
            fetch="all",
        )

    def _batches(self, project):
        return project.run_sql(
            f"select batch_id, n_insert, n_update, n_delete "
            f"from {self._c(project, 'batches')} order by batch_id",
            fetch="all",
        )

    def _progress(self, project):
        return {
            row[0]: (row[1], row[2])
            for row in project.run_sql(
                f"select consumer, last_seq, last_batch "
                f"from {self._c(project, 'progress')} order by consumer",
                fetch="all",
            )
        }

    @staticmethod
    def _write_source(project, body):
        write_file(
            "{{ config(materialized='table') }}\n" + body,
            project.project_root, "models", "source_rows.sql",
        )

    def test_change_stream_lifecycle(self, project):
        # ---- run 1: initial load, all rows are inserts ----
        self._write_source(project, SOURCE_ROWS_INITIAL)
        run_dbt(["run"])

        assert project.run_sql(
            f"select count(*) from {self._t(project, 'pub')}", fetch="one"
        )[0] == 2
        assert self._batches(project) == [(1, 2, 0, 0)]
        changes = self._changes(project)
        assert [r[0] for r in changes] == ["insert", "insert"]
        # after image populated, before null on insert
        assert changes[0][2] is None and changes[0][3] == '{"id":1,"name":"a","amt":10}'

        # both consumers independently advance to seq 2 / batch 1
        progress = self._progress(project)
        assert progress["model.test.consumer_replace"] == (2, 1)
        assert progress["model.test.consumer_append"] == (2, 1)

        # plain ref sees the business table only
        assert project.run_sql(
            f"select count(*) from {self._t(project, 'plain_ref')}", fetch="one"
        )[0] == 2

        # double read is consistent and bounded
        assert project.run_sql(
            f"select distinct n, hi from {self._t(project, 'double_read')}", fetch="all"
        ) == [(2, 2)]

        # ---- run 2: update id 1, delete id 2, insert id 3 ----
        # A full run (no --select) so dbt rebuilds the changed source_rows
        # dependency first; otherwise the publisher would read the stale table.
        self._write_source(project, SOURCE_ROWS_ROUND2)
        run_dbt(["run"])
        assert self._batches(project)[-1] == (2, 1, 1, 1)
        round2 = project.run_sql(
            f"select change_type, key from {self._c(project, 'changes')} "
            f"where batch_id = 2 order by seq",
            fetch="all",
        )
        assert round2 == [
            ("update", '{"id":1}'),
            ("delete", '{"id":2}'),
            ("insert", '{"id":3}'),
        ]
        upd = project.run_sql(
            f"select before_image, after_image from {self._c(project, 'changes')} "
            f"where batch_id = 2 and change_type = 'update'",
            fetch="one",
        )
        assert upd == ('{"id":1,"name":"a","amt":10}', '{"id":1,"name":"a2","amt":11}')

        # after the full run the consumers already read the new window; verify
        # replace mode holds exactly batch-2 seqs and append accumulated all 5
        replace_rows = project.run_sql(
            f"select seq, change_type, id from {self._t(project, 'consumer_replace')} "
            f"order by seq",
            fetch="all",
        )
        assert [r[0] for r in replace_rows] == [3, 4, 5]
        assert project.run_sql(
            f"select count(*) from {self._t(project, 'consumer_append')}", fetch="one"
        )[0] == 5
        assert all(v == (5, 2) for v in self._progress(project).values())

        # ---- re-run with no new publisher changes: no-op, no duplication ----
        run_dbt(["run", "--select", "consumer_replace", "consumer_append"])
        assert project.run_sql(
            f"select count(*) from {self._t(project, 'consumer_append')}", fetch="one"
        )[0] == 5
        assert all(v == (5, 2) for v in self._progress(project).values())

        # ---- run 3: empty result -> deletes plus a recorded batch ----
        self._write_source(project, SOURCE_ROWS_EMPTY)
        run_dbt(["run", "--select", "source_rows", "pub"])
        assert self._batches(project)[-1] == (3, 0, 0, 2)
        assert project.run_sql(
            f"select count(*) from {self._t(project, 'pub')}", fetch="one"
        )[0] == 0
        deletes = project.run_sql(
            f"select count(*) from {self._c(project, 'changes')} "
            f"where batch_id = 3 and change_type = 'delete'",
            fetch="one",
        )[0]
        assert deletes == 2

COMPOSITE_V1 = """
{{ config(materialized='change_stream', business_key=['region', 'order_id']) }}
select 'us' as region, 100 as order_id, 5 as qty union all
select 'eu', 100, 7 union all
select 'us', 101, 9
"""

# us/100 changes qty AND gains a column; eu/100 only gains a column (its shared
# columns are unchanged); us/101 is deleted; ap/200 is inserted.
COMPOSITE_V2 = """
{{ config(materialized='change_stream', business_key=['region', 'order_id']) }}
select 'us' as region, 100 as order_id, 6 as qty, 'priority' as tier union all
select 'eu', 100, 7, 'standard' union all
select 'ap', 200, 2, 'standard'
"""


class TestChangeStreamCompositeAndSchemaEvolution:
    @pytest.fixture(scope="class")
    def models(self):
        return {"orders.sql": COMPOSITE_V1}

    def _c(self, project, suffix):
        return f'''"{project.database}"."{CHANGE_SCHEMA}"."orders__{suffix}"'''

    def test_composite_key_and_schema_evolution(self, project):
        run_dbt(["run"])
        # evolve the model: rewrite it with a new shape and rerun
        write_file(
            COMPOSITE_V2, project.project_root, "models", "orders.sql",
        )
        run_dbt(["run"])

        batch2 = project.run_sql(
            f"select change_type, key from {self._c(project, 'changes')} "
            f"where batch_id = 2 order by seq",
            fetch="all",
        )
        # eu/100 must NOT appear: a pure added column with unchanged shared
        # columns is not an update. The OR-vs-AND join bug would cross-match
        # rows sharing only one key member and produce extra updates.
        assert batch2 == [
            ("update", '{"region":"us","order_id":100}'),
            ("delete", '{"region":"us","order_id":101}'),
            ("insert", '{"region":"ap","order_id":200}'),
        ]
        assert project.run_sql(
            f"select n_insert, n_update, n_delete from {self._c(project, 'batches')} "
            f"where batch_id = 2",
            fetch="one",
        ) == (1, 1, 1)
        # the live table carries the evolved schema
        assert project.run_sql(
            f"select count(*) from {self._t_orders(project)}", fetch="one"
        )[0] == 3

    def _t_orders(self, project):
        return f'''"{project.database}"."{project.test_schema}"."orders"'''


class TestChangeStreamFailureReplay:
    """A consumer whose in-transaction post hook fails must roll back its table
    and its progress cursor, then replay the identical window on the next run."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_rows.sql": "{{ config(materialized='table') }}\n" + SOURCE_ROWS_INITIAL,
            "pub.sql": PUBLISHER,
            "consumer_replace.sql": CONSUMER_REPLACE,
        }

    def _t(self, project, name):
        return f'''"{project.database}"."{project.test_schema}"."{name}"'''

    def _c(self, project, suffix):
        return f'''"{project.database}"."{CHANGE_SCHEMA}"."pub__{suffix}"'''

    def test_failure_replays_same_window(self, project):
        run_dbt(["run"])

        # publish one more change (id 3 inserted); rebuild the changed source too
        write_file(
            "{{ config(materialized='table') }}\n" + SOURCE_ROWS_ROUND2,
            project.project_root, "models", "source_rows.sql",
        )
        run_dbt(["run", "--select", "source_rows", "pub"])

        # failing in-transaction post hook
        write_file(
            CONSUMER_REPLACE_FAILING_HOOK,
            project.project_root, "models", "consumer_replace.sql",
        )
        run_dbt(["run", "--select", "consumer_replace"], expect_pass=False)

        # table and cursor both rolled back to the first-run position (seq 2)
        assert project.run_sql(
            f"select max(seq) from {self._t(project, 'consumer_replace')}", fetch="one"
        )[0] == 2
        assert project.run_sql(
            f"select last_seq from {self._c(project, 'progress')} "
            f"where consumer = 'model.test.consumer_replace'",
            fetch="one",
        )[0] == 2

        # fix the hook and replay: the waiting window is consumed exactly once
        write_file(
            CONSUMER_REPLACE,
            project.project_root, "models", "consumer_replace.sql",
        )
        run_dbt(["run", "--select", "consumer_replace"])
        assert project.run_sql(
            f"select count(*) from {self._t(project, 'consumer_replace')} where seq = 5",
            fetch="one",
        )[0] == 1
        assert project.run_sql(
            f"select last_seq from {self._c(project, 'progress')} "
            f"where consumer = 'model.test.consumer_replace'",
            fetch="one",
        )[0] == 5


class TestChangeStreamNullBusinessKey:
    """A row whose business key is NULL still has row identity: an unchanged key
    with a changed non-key column must publish as a single update, not a
    delete+insert. Classification uses a non-null presence sentinel so outer-join
    padding NULLs are never confused with a genuinely NULL key."""

    NULL_V1 = """
{{ config(materialized='change_stream', business_key='id') }}
select cast(null as integer) as id, 'x' as note union all
select 2, 'y'
"""

    NULL_V2 = """
{{ config(materialized='change_stream', business_key='id') }}
select cast(null as integer) as id, 'x2' as note union all
select 2, 'y'
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"nullkey.sql": self.NULL_V1}

    def _c(self, project, suffix):
        return f'''"{project.database}"."{CHANGE_SCHEMA}"."nullkey__{suffix}"'''

    def test_null_key_row_updates_in_place(self, project):
        run_dbt(["run"])
        write_file(self.NULL_V2, project.project_root, "models", "nullkey.sql")
        run_dbt(["run"])

        batch2 = project.run_sql(
            f"select change_type, key, before_image, after_image "
            f"from {self._c(project, 'changes')} where batch_id = 2 order by seq",
            fetch="all",
        )
        # The NULL-id row changed only `note`: exactly one update, no delete/insert.
        assert batch2 == [
            ("update", '{"id":null}',
             '{"id":null,"note":"x"}', '{"id":null,"note":"x2"}'),
        ]
        assert project.run_sql(
            f"select n_insert, n_update, n_delete from {self._c(project, 'batches')} "
            f"where batch_id = 2",
            fetch="one",
        ) == (0, 1, 0)


class TestChangeStreamSchemaMismatch:
    """A consumer whose change_schema does not match its publisher's must fail
    with an actionable error rather than silently reading an empty stream."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "pub.sql": (
                "{{ config(materialized='change_stream', business_key='id', "
                "change_schema='custom_cdc') }}\n"
                "select 1 as id union all select 2"
            ),
            "wrong_consumer.sql": (
                "{{ config(materialized='stream_consumer', stream_of='pub') }}\n"
                "select seq, change_type from {{ stream_changes('pub') }}"
            ),
        }

    def test_mismatch_is_rejected(self, project):
        run_dbt(["run", "--select", "pub"])
        result = run_dbt(["run", "--select", "wrong_consumer"], expect_pass=False)
        joined = " ".join(str(r) for r in result.results)
        assert "change_schema='custom_cdc'" in joined

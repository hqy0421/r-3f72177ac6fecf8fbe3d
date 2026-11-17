-- SQL that publishes one change batch for a change_stream model.
--
-- It compares the freshly-built intermediate table (`new`) against the live
-- business table (`old`) on the declared business key and appends one row per
-- real row change: insert / update / delete, each with JSON before/after images
-- and a typed business-key struct. An empty new table yields delete rows (and a
-- batch row) rather than nothing.
--
-- Everything here runs inside the materialization transaction.

{% macro duckdb_cdc_publish_changes(
      target_relation, intermediate_relation, existing_relation,
      key_columns, new_columns, old_columns, common_columns,
      changes_relation, batches_relation, sequence_name) %}

  {%- set source_id = duckdb_cdc_source_id(target_relation) -%}

  {%- if existing_relation is none -%}
    {#-- First publication: the whole new table is inserts. --#}
    insert into {{ changes_relation }}
    select
      nextval('{{ sequence_name }}'),
      '{{ source_id }}',
      {{ duckdb_cdc_current_batch_id(batches_relation) }},
      'insert',
      {{ duckdb_cdc_json_image('new', key_columns) }},
      null::json,
      {{ duckdb_cdc_json_image('new', new_columns) }},
      now()
    from {{ intermediate_relation }} new
  {%- else -%}
    {#-- Subsequent publications: full outer join on the business key. Each side
         emits a non-null sentinel so a row that exists but has NULL key members
         is distinguishable from the NULL padding the outer join adds to an
         unmatched side. Classifying on the sentinel (not on key nullness) is
         what lets a NULL-key row match and update rather than look inserted. --#}
    insert into {{ changes_relation }}
    with joined as (
      select
        {{ duckdb_cdc_json_image('old', key_columns) }} as old_key,
        {{ duckdb_cdc_json_image('new', key_columns) }} as new_key,
        {{ duckdb_cdc_json_image('old', old_columns) }} as old_image,
        {{ duckdb_cdc_json_image('new', new_columns) }} as new_image,
        {{ duckdb_cdc_struct_pack('old', common_columns) }} as old_cmp,
        {{ duckdb_cdc_struct_pack('new', common_columns) }} as new_cmp,
        old.__dbt_cdc_row_present__ as old_present,
        new.__dbt_cdc_row_present__ as new_present
      from (
        select *, 1 as __dbt_cdc_row_present__ from {{ existing_relation }}
      ) old
      full outer join (
        select *, 1 as __dbt_cdc_row_present__ from {{ intermediate_relation }}
      ) new
        on {{ duckdb_cdc_join_on(key_columns) }}
    )
    select
      nextval('{{ sequence_name }}'),
      '{{ source_id }}',
      {{ duckdb_cdc_current_batch_id(batches_relation) }},
      classified.change_type,
      case
        when classified.change_type = 'insert' then classified.new_key
        else classified.old_key
      end,
      case
        when classified.change_type = 'insert' then null::json
        else classified.old_image
      end,
      case
        when classified.change_type = 'delete' then null::json
        else classified.new_image
      end,
      now()
    from (
      select
        joined.*,
        case
          when joined.old_present is null then 'insert'
          when joined.new_present is null then 'delete'
          when joined.old_cmp is distinct from joined.new_cmp then 'update'
        end as change_type
      from joined
    ) classified
    where classified.change_type is not null
  {%- endif -%}
{%- endmacro %}


-- Insert the batch summary row using the counts actually written this run.
-- The batch row is always inserted, even when the change set is empty.
{% macro duckdb_cdc_record_batch(target_relation, changes_relation, batches_relation) %}
  {%- set source_id = duckdb_cdc_source_id(target_relation) -%}
  insert into {{ batches_relation }}
  select
    {{ duckdb_cdc_current_batch_id(batches_relation) }},
    '{{ source_id }}',
    count(*) filter (where change_type = 'insert'),
    count(*) filter (where change_type = 'update'),
    count(*) filter (where change_type = 'delete'),
    now()
  from {{ changes_relation }}
  where batch_id = {{ duckdb_cdc_current_batch_id(batches_relation) }}
{% endmacro %}


-- Monotonic per-source batch id: one past the max already recorded.
{% macro duckdb_cdc_current_batch_id(batches_relation) %}
  (select coalesce(max(batch_id), 0) + 1 from {{ batches_relation }})
{% endmacro %}

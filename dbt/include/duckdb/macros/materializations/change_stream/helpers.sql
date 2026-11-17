-- Model change stream (CDC) support for local DuckDB.
--
-- A `change_stream` model is still materialized as an ordinary business table
-- (so ordinary `ref()` keeps working), but every rebuild also publishes the
-- row-level difference between the old and the new table into a per-source
-- change log. Downstream `stream_consumer` models read that log from their own
-- committed progress up to a fixed upper bound, so replays and failures cannot
-- skip or double-count changes.
--
-- All bookkeeping lives in a separate change schema and is written inside the
-- same transaction as the business-table swap, so a failed run rolls back
-- everything together.

{% macro duckdb_cdc_default_change_schema() %}
  {{ return(var('duckdb_change_stream_schema', '_dbt_changes')) }}
{% endmacro %}

-- Normalize the declared business key to a list of quoted-free column names.
{% macro duckdb_cdc_normalize_key(raw_key) %}
  {% if raw_key is string %}
    {% set keys = [raw_key] %}
  {% elif raw_key is iterable %}
    {% set keys = raw_key | list %}
  {% else %}
    {{ exceptions.raise_compiler_error(
      "change_stream 'business_key' must be a column name or a list of column names, got: " ~ raw_key) }}
  {% endif %}
  {% if keys | length == 0 %}
    {{ exceptions.raise_compiler_error(
      "change_stream requires a non-empty 'business_key' to compare old and new rows") }}
  {% endif %}
  {{ return(keys) }}
{% endmacro %}

-- Build a DuckDB relation pointing at a change-stream control table.
{% macro duckdb_cdc_control_relation(target_relation, suffix) %}
  {% set change_schema = config.get('change_schema', duckdb_cdc_default_change_schema()) %}
  {{ return(api.Relation.create(
       database=target_relation.database,
       schema=change_schema,
       identifier=target_relation.identifier ~ suffix,
       type='table')) }}
{% endmacro %}

{% macro duckdb_cdc_changes_relation(target_relation) %}
  {{ return(duckdb_cdc_control_relation(target_relation, '__changes')) }}
{% endmacro %}

{% macro duckdb_cdc_batches_relation(target_relation) %}
  {{ return(duckdb_cdc_control_relation(target_relation, '__batches')) }}
{% endmacro %}

{% macro duckdb_cdc_progress_relation(target_relation) %}
  {{ return(duckdb_cdc_control_relation(target_relation, '__progress')) }}
{% endmacro %}

{% macro duckdb_cdc_sequence_name(target_relation) %}
  {{ return(target_relation.database ~ '.' ~
            config.get('change_schema', duckdb_cdc_default_change_schema()) ~ '.' ~
            '"' ~ target_relation.identifier ~ '__seq"') }}
{% endmacro %}

-- A stable identifier of the published source (the fully qualified business table).
{% macro duckdb_cdc_source_id(target_relation) %}
  {{ return(target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.identifier) }}
{% endmacro %}

-- struct_pack over the given (alias, columns); used for the typed business key.
{% macro duckdb_cdc_struct_pack(alias, columns) %}
  struct_pack(
    {%- for c in columns -%}
      "{{ c }}" := {{ alias }}."{{ c }}"{{ ", " if not loop.last }}
    {%- endfor -%}
  )
{% endmacro %}

-- JSON before/after image. JSON keeps a full, typed-ish row snapshot even when
-- the model schema evolves between runs; values are read back typed with `::`.
{% macro duckdb_cdc_json_image(alias, columns) %}
  to_json({{ duckdb_cdc_struct_pack(alias, columns) }})
{% endmacro %}

-- Join predicate matching old/new rows on every business-key column.
-- All key columns must match (AND), so a composite key only joins rows whose
-- full key is equal; an OR would cross-match rows that share just one member.
-- IS NOT DISTINCT FROM treats NULL key members as equal.
{% macro duckdb_cdc_join_on(key_columns) %}
  {%- for c in key_columns -%}
    old."{{ c }}" is not distinct from new."{{ c }}"{{ " and " if not loop.last }}
  {%- endfor -%}
{% endmacro %}

-- Columns present in both the old and the new table. Update detection compares
-- only these: a pure schema change that adds/removes a column must not make an
-- otherwise unchanged row look like an update.
{% macro duckdb_cdc_common_columns(old_columns, new_columns) %}
  {%- set common = [] -%}
  {%- for c in new_columns -%}
    {%- if c in old_columns -%}
      {%- do common.append(c) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(common) }}
{% endmacro %}

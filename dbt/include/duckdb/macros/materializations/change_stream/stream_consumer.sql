-- stream_consumer materialization
--
-- A downstream model that consumes a change stream. Each consumer has its own
-- progress cursor (in the source's __progress table, keyed by the model's
-- unique_id). A run reads exactly the changes in (last_seq, upper_seq], where
-- upper_seq is a fixed bound captured once for the invocation. The cursor is
-- advanced to upper_seq only after the model table and the in-transaction post
-- hooks succeed; on failure everything rolls back and the next run replays the
-- same window, so no change is lost or double-counted.
--
-- Required config:
--   stream_of : the node name (string) of a change_stream model, e.g.
--               stream_of='my_publisher'; or a 'database.schema.table' string.
--               A relation is also accepted by stream_changes() directly.
-- Optional config:
--   consumer_mode : 'replace' (default) — rebuild the whole model from the
--                   bounded change window each run; or 'append' — append the
--                   new window's rows to the existing model table.
--   change_schema : override the change-stream control schema.

{% materialization stream_consumer, adapter="duckdb", supported_languages=['sql'] %}

  {%- set language = model['language'] -%}
  {%- set source_relation = duckdb_cdc_resolve_consumer_source() -%}
  {%- set source_id = source_relation.database ~ '.' ~ source_relation.schema ~ '.' ~ source_relation.identifier -%}
  {%- set change_schema = config.get('change_schema', duckdb_cdc_default_change_schema()) -%}
  {%- set changes_relation = duckdb_cdc_changes_relation(source_relation) -%}
  {%- set progress_relation = duckdb_cdc_progress_relation(source_relation) -%}
  {%- set consumer = model.unique_id -%}
  {%- set consumer_mode = config.get('consumer_mode', 'replace') -%}
  {%- if consumer_mode not in ['replace', 'append'] -%}
    {{ exceptions.raise_compiler_error(
      "stream_consumer 'consumer_mode' must be 'replace' or 'append', got: " ~ consumer_mode) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup = load_cached_relation(backup_relation) -%}
  {%- set grant_config = config.get('grants') -%}

  {{ drop_relation_if_exists(preexisting_intermediate) }}
  {{ drop_relation_if_exists(preexisting_backup) }}

  {#-- Compute the read window before BEGIN; the upper bound is cached so the
       compiled query (rendered with stream_changes) uses these same values. --#}
  {%- set last_seq, upper_seq = adapter.change_stream_read_window(
        changes_relation, progress_relation, source_id, consumer) -%}

  {#-- Fail loudly on a change_schema mismatch rather than silently reading an
       empty/missing stream. Only treat the source as genuinely unpublished when
       the changes table exists nowhere for this identifier. --#}
  {%- if not adapter.change_stream_relation_exists(changes_relation) -%}
    {%- set elsewhere = adapter.change_stream_find_control_schema(
          source_relation.database, source_relation.identifier ~ '__changes') -%}
    {%- if elsewhere is not none -%}
      {{ exceptions.raise_compiler_error(
        "stream_consumer '" ~ model.unique_id ~ "' looks for the change stream of '"
        ~ source_id ~ "' in schema '" ~ change_schema ~ "', but its change table lives in '"
        ~ elsewhere ~ "'. Set change_schema='" ~ elsewhere ~ "' on this consumer to match "
        ~ "the publisher's change_schema.") }}
    {%- endif -%}
  {%- endif -%}

  {%- set has_new_changes = upper_seq > last_seq -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if has_new_changes %}
    {#-- Build the model from its compiled SQL, which reads via stream_changes
         and therefore sees exactly (last_seq, upper_seq]. --#}
    {% if consumer_mode == 'append' and existing_relation is not none %}
      {% call statement('main', language=language) %}
        insert into {{ target_relation }}
        {{ compiled_code }}
      {% endcall %}
    {% else %}
      {% call statement('main', language=language) %}
        {{ create_table_as(False, intermediate_relation, compiled_code, language) }}
      {% endcall %}
      {% if existing_relation is not none %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
      {% endif %}
      {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {% endif %}

    {% do create_indexes(target_relation) %}

    {#-- In-transaction post hooks run while the build is still uncommitted; a
         hook failure rolls back both the table and (below) the cursor. --#}
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
    {% do persist_docs(target_relation, model) %}

    {#-- Advance the cursor inside the same transaction, last thing before commit. --#}
    {% do adapter.change_stream_advance_progress(
          progress_relation, consumer, upper_seq, changes_relation) %}

    -- `COMMIT` happens here: model table + progress cursor advance together
    {{ adapter.commit() }}

    {{ drop_relation_if_exists(backup_relation) }}
  {% else %}
    {#-- Nothing new since the last successful run: do not rebuild or touch the
         cursor. A statement named 'main' is still required by dbt's runner, so
         issue a harmless no-op SELECT. --#}
    {% call statement('main') %}
      select 1
    {% endcall %}
    {{ adapter.commit() }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

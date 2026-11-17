-- change_stream materialization
--
-- Rebuilds the model as an ordinary business table (so plain `ref()` keeps
-- working exactly like the `table` materialization) and, in the SAME
-- transaction, publishes the row-level difference between the old and new table
-- to a per-source change log. The business-table swap and the change batch
-- commit atomically: a failure rolls back both, so no batch is ever published
-- without its table, and no table change is ever left unpublished.
--
-- Required config:
--   business_key  : column name (string) or list of column names identifying a
--                   business row. Old and new rows sharing a key are compared;
--                   only real differences are recorded as updates.
-- Optional config:
--   change_schema : schema holding the change log / batch / progress tables
--                   (default '_dbt_changes', override via var or model config).

{% materialization change_stream, adapter="duckdb", supported_languages=['sql'] %}

  {%- set language = model['language'] -%}
  {%- set raw_key = config.get('business_key') -%}
  {%- if raw_key is none -%}
    {{ exceptions.raise_compiler_error(
      "Model '" ~ model.unique_id ~ "' uses materialization='change_stream' but does not "
      ~ "declare a 'business_key'. Add business_key='<col>' (or a list) to its config.") }}
  {%- endif -%}
  {%- set key_columns = duckdb_cdc_normalize_key(raw_key) -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set changes_relation = duckdb_cdc_changes_relation(target_relation) -%}
  {%- set batches_relation = duckdb_cdc_batches_relation(target_relation) -%}
  {%- set progress_relation = duckdb_cdc_progress_relation(target_relation) -%}
  {%- set sequence_name = duckdb_cdc_sequence_name(target_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- control tables are created inside the transaction so rollback undoes them too
  {% call statement('bootstrap_cdc') %}
    {{ duckdb_cdc_bootstrap(target_relation) }}
  {% endcall %}

  -- build the new model state in the intermediate relation
  {% call statement('main', language=language) %}
    {{ create_table_as(False, intermediate_relation, compiled_code, language) }}
  {% endcall %}

  -- column lists drive the before/after images; old columns may differ from new
  -- ones after a schema change, so each side describes its own table.
  {%- set new_columns = adapter.get_columns_in_relation(intermediate_relation) | map(attribute='name') | list -%}
  {%- set old_columns = new_columns -%}
  {%- if existing_relation is not none -%}
    {%- set old_columns = adapter.get_columns_in_relation(existing_relation) | map(attribute='name') | list -%}
  {%- endif -%}
  {%- set common_columns = duckdb_cdc_common_columns(old_columns, new_columns) -%}
  {%- for k in key_columns -%}
    {%- if k not in new_columns -%}
      {{ exceptions.raise_compiler_error(
        "business_key column '" ~ k ~ "' is not in the columns of model '" ~ model.unique_id
        ~ "': " ~ new_columns) }}
    {%- endif -%}
  {%- endfor -%}

  -- publish the row-level changes (inserts / updates / deletes)
  {% call statement('publish_changes') %}
    {{ duckdb_cdc_publish_changes(
         target_relation, intermediate_relation, existing_relation,
         key_columns, new_columns, old_columns, common_columns,
         changes_relation, batches_relation, sequence_name) }}
  {% endcall %}

  -- always record the batch, even when the change set is empty
  {% call statement('record_batch') %}
    {{ duckdb_cdc_record_batch(target_relation, changes_relation, batches_relation) }}
  {% endcall %}

  -- swap the rebuilt table into place (rollback restores the old table on failure)
  {% if existing_relation is not none %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here: business table + change batch become visible together
  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

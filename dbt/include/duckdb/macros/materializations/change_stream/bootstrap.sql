-- Bootstrap the per-source change-stream control tables and sequence.
-- Idempotent: safe to run on every publication. All objects live in the change
-- schema, separate from the business schema, so ordinary refs never see them.

{% macro duckdb_cdc_ensure_change_schema(target_relation) %}
  {%- set change_schema_relation = api.Relation.create(
       database=target_relation.database,
       schema=config.get('change_schema', duckdb_cdc_default_change_schema())) -%}
  {#-- create_schema renders `create schema if not exists`, so this is idempotent
       and avoids depending on agate-table membership semantics. --#}
  {% do run_query(create_schema(change_schema_relation)) %}
{% endmacro %}

{% macro duckdb_cdc_bootstrap(target_relation) %}
  {%- set changes_relation = duckdb_cdc_changes_relation(target_relation) -%}
  {%- set batches_relation = duckdb_cdc_batches_relation(target_relation) -%}
  {%- set progress_relation = duckdb_cdc_progress_relation(target_relation) -%}
  {%- set sequence_name = duckdb_cdc_sequence_name(target_relation) -%}

  {% do duckdb_cdc_ensure_change_schema(target_relation) %}

  create table if not exists {{ changes_relation }} (
    seq          bigint not null,
    source_id    varchar not null,
    batch_id     bigint not null,
    change_type  varchar not null,
    key          json not null,
    before_image json,
    after_image  json,
    changed_at   timestamp not null
  );

  create table if not exists {{ batches_relation }} (
    batch_id     bigint not null,
    source_id    varchar not null,
    n_insert     bigint not null,
    n_update     bigint not null,
    n_delete     bigint not null,
    published_at timestamp not null
  );

  create table if not exists {{ progress_relation }} (
    consumer     varchar not null primary key,
    last_seq     bigint not null,
    last_batch   bigint not null,
    updated_at   timestamp not null
  );

  create sequence if not exists {{ sequence_name }} start 1;
{% endmacro %}

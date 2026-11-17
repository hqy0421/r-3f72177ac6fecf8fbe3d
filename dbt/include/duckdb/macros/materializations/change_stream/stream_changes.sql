-- Downstream entry point: read the change stream of a source.
--
-- `stream_changes(source)` returns a relation over the changes the current
-- consumer model has not yet read, bounded at a fixed upper seq so the model's
-- compiled query always sees one stable window even if the publisher commits
-- during the build. The consumer's progress cursor is advanced only when the
-- model (and its in-transaction post hooks) succeeds and commits.
--
-- Usage inside a `stream_consumer` model's SQL (the source may be passed as a
-- ref() relation or as its node-name string):
--
--   select * from {{ stream_changes(ref('my_change_stream_model')) }}
--   select * from {{ stream_changes('my_change_stream_model') }}
--
-- Returned columns:
--   seq, batch_id, change_type ('insert'|'update'|'delete'),
--   key (json), before_image (json|null), after_image (json|null), changed_at
-- Read image fields back with typed extraction, e.g.
--   after_image.id::integer   or   (after_image ->> 'amount')::decimal(12,2)

{% macro stream_changes(source) %}
  {%- if source is string -%}
    {%- if source.count('.') == 2 -%}
      {%- set parts = source.split('.') -%}
      {%- set source_relation = api.Relation.create(
            database=parts[0], schema=parts[1], identifier=parts[2], type='table') -%}
    {%- else -%}
      {%- set source_relation = ref(source).incorporate(type='table') -%}
    {%- endif -%}
  {%- else -%}
    {%- set source_relation = source.incorporate(type='table') -%}
  {%- endif -%}
  {%- set source_id = source_relation.database ~ '.' ~ source_relation.schema ~ '.' ~ source_relation.identifier -%}
  {%- set changes_relation = duckdb_cdc_changes_relation(source_relation) -%}
  {%- set progress_relation = duckdb_cdc_progress_relation(source_relation) -%}
  {%- set consumer = model.unique_id -%}

  {#-- Resolve the window at compile time. The upper bound is cached per
       (source, consumer) for the whole invocation, so every reference inside
       this model renders the same literal and cannot drift across batches. --#}
  {%- set last_seq, upper_seq = adapter.change_stream_read_window(
        changes_relation, progress_relation, source_id, consumer) -%}

  (
    select seq, batch_id, change_type, key, before_image, after_image, changed_at
    from {{ changes_relation }}
    where seq > {{ last_seq }}
      and seq <= {{ upper_seq }}
  )
{%- endmacro %}


-- Resolve and validate the source model a stream_consumer points at.
-- `stream_of` is the only required config. It holds a *node name* (a plain
-- string, so the parsed manifest stays serializable); the relation is resolved
-- here at render time via ref(). A fully-qualified 'database.schema.table'
-- string is also accepted for sources not modeled in this dbt project.
{% macro duckdb_cdc_resolve_consumer_source() %}
  {%- set stream_of = config.get('stream_of') -%}
  {%- if stream_of is none -%}
    {{ exceptions.raise_compiler_error(
      "Model '" ~ model.unique_id ~ "' uses materialization='stream_consumer' but does not "
      ~ "declare 'stream_of'. Set stream_of='<change_stream_model_name>' in its config.") }}
  {%- endif -%}
  {%- if stream_of is string and stream_of.count('.') == 2 -%}
    {%- set parts = stream_of.split('.') -%}
    {%- set source_relation = api.Relation.create(
          database=parts[0], schema=parts[1], identifier=parts[2], type='table') -%}
  {%- elif stream_of is string -%}
    {%- set source_relation = ref(stream_of).incorporate(type='table') -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error(
      "stream_of must be the string name of a change_stream model "
      ~ "(or a 'database.schema.table' string), got: " ~ stream_of) }}
  {%- endif -%}
  {{ return(source_relation) }}
{% endmacro %}


-- Convenience used inside a consumer's compiled SQL: render the bounded change
-- relation for the configured source without passing the relation through config.
{% macro duckdb_cdc_configured_source_relation() %}
  {{ return(duckdb_cdc_resolve_consumer_source()) }}
{% endmacro %}

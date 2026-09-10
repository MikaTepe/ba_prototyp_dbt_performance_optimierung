{% macro snapshot_merge_sql(target, source, insert_cols) %}
    {%- set strategy_name = config.get('strategy') -%}
    {% set strategy_macro = strategy_dispatch(strategy_name) %}
    {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", model['config'], target_relation_exists) %}

    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set cols_with_source = [] -%}
    {%- for col in insert_cols -%}
        {%- do cols_with_source.append('DBT_INTERNAL_SOURCE.' ~ col) -%}
    {%- endfor -%}
    {%- set insert_cols_csv_src = cols_with_source | join(', ') -%}


    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{ silver_merge_key_condition("DBT_INTERNAL_DEST", "DBT_INTERNAL_SOURCE", strategy.key_cols) }}
    and DBT_INTERNAL_DEST.{{ strategy.temporal_cols.get('bis') }} = {{ strategy.temp_max }}
    and DBT_INTERNAL_SOURCE.dbt_change_type <> 'insert'

    when matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'delete'
    then delete

    when matched
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'invalidate')
        then update
        set {{ strategy.temporal_cols.get('bis') }} = {{ strategy.on_change_value }}
          , aend_zeit = current_timestamp

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv_src }})
{% endmacro %}
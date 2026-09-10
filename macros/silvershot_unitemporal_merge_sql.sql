{% macro silvershot_unitemporal_merge_sql(target, source, strategy, insert_cols) %}

    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set cols_with_source = [] -%}
    {%- for col in insert_cols -%}
        {%- do cols_with_source.append('DBT_INTERNAL_SOURCE.' ~ col) -%}
    {%- endfor -%}
    {%- set insert_cols_csv_src = cols_with_source | join(', ') -%}
    {{ log("insert_cols_src: " ~ cols_with_source|string, info=True) }}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{ strategy.source_unique_key }} = {{ strategy.dest_unique_key }}
    and DBT_INTERNAL_SOURCE.change_flag_col <> 'insert'

    when matched
     and DBT_INTERNAL_DEST.bis = {{ strategy.temp_max }}
     and DBT_INTERNAL_SOURCE.change_flag_col in ('update', 'delete')
        then update
        set bis = {{ strategy.on_change_value }}
          , aend_zeit = current_timestamp

    when not matched
     and DBT_INTERNAL_SOURCE.change_flag_col = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv_src }})


{% endmacro %}
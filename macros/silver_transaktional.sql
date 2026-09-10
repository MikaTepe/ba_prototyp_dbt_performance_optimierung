{% macro get_incremental_silver_transaktional_sql(arg_dict) %}

  {% do return(
      silver_transaktional_sql(
        arg_dict["target_relation"],
        arg_dict["temp_relation"],
        arg_dict["dest_columns"],
        arg_dict["unique_key"]
      )
  ) %}

{% endmacro %}


{% macro silver_transaktional_sql(target_relation, temp_relation, dest_columns, unique_key) %}

  {% set clean_table_raw = var('CLEAN_TABLE', 'N') %}
  {% set clean_table = clean_table_raw | string | upper | trim %}

  {% if clean_table not in ['N', 'S', 'F'] %}
    {{ exceptions.raise_compiler_error(
        "Invalid CLEAN_TABLE value for silver_stichtag. Allowed values are N, S, F. Got: " ~ clean_table_raw
    ) }}
  {% endif %}

  {% set temporal_column_name = silver_stichtag_temporal_column() %}

  {% if execute %}

    {% set ignore_cols = ['aend_zeit'] %}

    {% set target_temporal_col = silver_stichtag__get_column_case_insensitive(
        target_relation,
        temporal_column_name,
        'target relation'
    ) %}

    {% do silver_stichtag__validate_temp_columns(
        temp_relation,
        dest_columns,
        target_temporal_col.name,
        ignore_cols
    ) %}

    {% set temporal_column_sql = adapter.quote(target_temporal_col.name) %}
    {% set temporal_data_type = target_temporal_col.data_type %}
    {% set resolved_temporal_column_name = target_temporal_col.name %}

  {% else %}

    {% set temporal_column_sql = adapter.quote(temporal_column_name) %}
    {% set temporal_data_type = 'date' %}
    {% set resolved_temporal_column_name = temporal_column_name %}

  {% endif %}

  {% set temporal_value_sql = silver_stichtag_temporal_value(temporal_data_type) %}
  {% set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) %}

  {% set load_filter_condition = resolve_load_filter_condition('tmp') %}

  {{ log("load_filter_condition: " ~ load_filter_condition|string, info=true) }}

  {% do silver_temporal_check_duplicates(
    target_relation=target_relation,
    temp_relation=temp_relation,
    unique_key=unique_key,
    temporal_column_sql=temporal_column_sql,
    temporal_value_sql=temporal_value_sql,
    clean_table=clean_table,
    load_filter_sql=load_filter_sql,
    strategy_name=strategy_name
  ) %}

  {% if clean_table == 'N' %}

    insert into {{ target_relation }} (
      {{ dest_cols_csv }}
    )
    select
      {% for col in dest_columns %}
        {% if col.name | lower == resolved_temporal_column_name | lower %}
          {{ temporal_value_sql }}
        {% elif col.name | lower == 'aend_zeit' %}
          cast(current_timestamp as timestamp(6)) as {{ adapter.quote(col.name) }}
        {% else %}
          {{ adapter.quote(col.name) }}
        {% endif %}
        {% if not loop.last %}, {% endif %}
      {% endfor %}
    from {{ temp_relation }}
    {% if load_filter_condition is not none %}
        where {{ load_filter_source_condition }}
    {% endif %}

  {% else %}

    merge into {{ target_relation }} as DBT_INTERNAL_DEST
    using (

      select
          {% if clean_table == 'S' %}
            '__dbt_delete_stichtag'
          {% else %}
            '__dbt_delete_all'
          {% endif %}
          as __dbt_silver_action,

          {{ temporal_value_sql }} as __dbt_temporal_value

          {% for col in dest_columns %}
          , cast(null as {{ col.data_type }}) as {{ adapter.quote(col.name) }}
          {% endfor %}

      union all

      select
          '__dbt_insert' as __dbt_silver_action,
          cast(null as {{ temporal_data_type }}) as __dbt_temporal_value

          {% for col in dest_columns %}
            {% if col.name | lower == resolved_temporal_column_name | lower %}
          , {{ temporal_value_sql }} as {{ adapter.quote(col.name) }}
            {% elif col.name | lower == 'aend_zeit' %}
          ,    cast(current_timestamp as timestamp(6)) as {{ adapter.quote(col.name) }}
            {% else %}
          , {{ adapter.quote(col.name) }} as {{ adapter.quote(col.name) }}
            {% endif %}
          {% endfor %}

      from {{ temp_relation }}
      {% if load_filter_condition is not none %}
        where {{ load_filter_condition }}
    {% endif %}

    ) as DBT_INTERNAL_SOURCE
    on (
      {% if clean_table == 'S' %}
        DBT_INTERNAL_SOURCE.__dbt_silver_action = '__dbt_delete_stichtag'
        and DBT_INTERNAL_DEST.{{ temporal_column_sql }} = DBT_INTERNAL_SOURCE.__dbt_temporal_value
        {% if load_filter_condition is not none %}
          and DBT_INTERNAL_DEST.{{ load_filter_condition }}
        {% endif %}
      {% elif clean_table == 'F' %}
        DBT_INTERNAL_SOURCE.__dbt_silver_action = '__dbt_delete_all'
        {% if load_filter_condition is not none %}
          and DBT_INTERNAL_DEST.{{ load_filter_condition }}
        {% endif %}

      {% endif %}
    )

    when matched
         and DBT_INTERNAL_SOURCE.__dbt_silver_action in ('__dbt_delete_stichtag', '__dbt_delete_all')
      then delete

    when not matched
         and DBT_INTERNAL_SOURCE.__dbt_silver_action = '__dbt_insert'
      then insert (
        {{ dest_cols_csv }}
      )
      values (
        {% for col in dest_columns %}
          DBT_INTERNAL_SOURCE.{{ adapter.quote(col.name) }}
          {% if not loop.last %}, {% endif %}
        {% endfor %}
      )

  {% endif %}

{% endmacro %}
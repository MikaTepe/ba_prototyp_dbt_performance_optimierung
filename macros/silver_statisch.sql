{% macro get_incremental_silver_statisch_sql(arg_dict) %}

  {% do return(
      silver_statisch_sql(
        arg_dict["target_relation"],
        arg_dict["temp_relation"],
        arg_dict["dest_columns"]
      )
  ) %}

{% endmacro %}


{% macro silver_statisch_sql(target_relation, temp_relation, dest_columns) %}

  {% set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) %}

  {% set load_filter_condition = resolve_load_filter_condition('tmp') %}

  {{ log("load_filter_condition: " ~ load_filter_condition|string, info=true) }}

    merge into {{ target_relation }} as DBT_INTERNAL_DEST
    using (

      select
            '__dbt_delete_all'
          as __dbt_silver_action

          {% for col in dest_columns %}
          , cast(null as {{ col.data_type }}) as {{ adapter.quote(col.name) }}
          {% endfor %}

      union all

      select
          '__dbt_insert' as __dbt_silver_action
          
          {% for col in dest_columns %}
            {% if col.name == 'aend_zeit' %}
                , cast(current_timestamp as timestamp(6)) as {{ adapter.quote(col.name) }}
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
        DBT_INTERNAL_SOURCE.__dbt_silver_action = '__dbt_delete_all'
        {% if load_filter_condition is not none %}
          and DBT_INTERNAL_DEST.{{ load_filter_condition }}
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

{% endmacro %}
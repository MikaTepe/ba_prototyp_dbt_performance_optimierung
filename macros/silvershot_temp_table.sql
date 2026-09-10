

{% macro build_fill_persistent_temp_table_sql(target, select_sql, insert_cols) %}
    {% if target is none %}
        {% do exceptions.raise_compiler_error(
            "build_fill_persistent_temp_table_sql: target relation is none"
        ) %}
    {% endif %}

    {% if select_sql is none or (select_sql | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "build_fill_persistent_temp_table_sql: select_sql is empty"
        ) %}
    {% endif %}

    {% if insert_cols is string %}
        {% set insert_cols = [insert_cols] %}
    {% endif %}

    {% if insert_cols is none or (insert_cols | length) == 0 %}
        {% do exceptions.raise_compiler_error(
            "build_fill_persistent_temp_table_sql: insert_cols must contain at least one column"
        ) %}
    {% endif %}

    {% set insert_cols_csv = insert_cols | join(', ') %}

    {% set sql %}
        insert into {{ target }} (
            {{ insert_cols_csv }}
        )
        {{ select_sql }}
    {% endset %}

    {{ return(sql) }}
{% endmacro %}


{% macro silver_temp_table(strategy, source_sql, target_relation) -%}
    {% if strategy.name == "unitemporal" %}
        {{ return(silver_unitemporal_temp_table(strategy, source_sql, target_relation)) }}
    {% elif strategy.name == "bitemporal" %}
        {{ return(silver_bitemporal_temp_table(strategy, source_sql, target_relation)) }}
    {% endif %}
{% endmacro %}
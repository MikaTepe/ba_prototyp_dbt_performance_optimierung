{% macro silvershot_merge_sql(target, source, strategy, insert_cols) %}

    {% if strategy.name == 'unitemporal' %}
        {{ return(silvershot_unitemporal_merge_sql(target, source, strategy, insert_cols)) }}
    {% elif strategy.name == 'bitemporal' %}
        {{ return(silvershot_bitemporal_merge_sql(target, source, strategy, insert_cols)) }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "Unsupported silvershot strategy: " ~ strategy.name
        ) %}
    {% endif %}

{% endmacro %}
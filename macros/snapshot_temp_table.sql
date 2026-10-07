{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {% if strategy.name == "unitemporal" %}
        {{ return(snapshot_staging_table_silver_unitemporal(strategy, source_sql, target_relation)) }}
    {% elif strategy.name == "bitemporal" %}
        {{ return(snapshot_staging_table_silver_bitemporal(strategy, source_sql, target_relation)) }}
    {% endif %}
{% endmacro %}
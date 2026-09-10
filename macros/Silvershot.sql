{% materialization Silvershot, default %}
    {# Model Konfiguration extrahieren #}
    {% set config = model['config'] %}

    {# Identifier von Zieltabelle extrahieren #}
    {% set target_table = config.get('meta').get('TAB_FKEY') %}

    {# Zielrelation deklarieren #}
    {% set target_relation = adapter.get_relation(
        database= model.database,
        schema=model.schema,
        identifier=target_table
    ) %}

    {# Identifier von TEMP_Tabelle extrahieren #}
    {% set temp_table = config.get('meta').get('TAB_FKEY') ~ '__dbt_tmp' %}

    {% set temp_relation = adapter.get_relation(
        database= model.database,
        schema=model.schema,
        identifier=temp_table
    ) %}

    {% set target_relation_exists = target_relation is not none %}
    {% set temp_relation_exists = temp_relation is not none %}

    {# target muss schon vorher existieren #}
    {% if not target_relation_exists %}
        {% do exceptions.raise_compiler_error(
          "Target table '" ~ target_table ~ "' does not exist. "
          ~ "This materialization is not allowed to create artifacts."
        ) %}
    {% endif %}

    {# temp Tabelle muss vorher existieren #}
    {% if not temp_relation_exists %}
        {% do exceptions.raise_compiler_error(
          "Persistent temp table '" ~ temp_table ~ "' does not exist. "
          ~ "This materialization requires a pre-provisioned temp table."
        ) %}
    {% endif %}

    {# sicherstellen, dass Zielrelation und Temprelation existieren #}
    {% if not target_relation.is_table %}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {% endif %}
    {% if not temp_relation.is_table %}
        {% do exceptions.relation_wrong_type(temp_relation, 'table') %}
    {% endif %}


    {% set compiled_sql = model['compiled_code'] %}

    {% set strategy_name = config.get('strategy') %}
    {% set unique_key = config.get('unique_key') %}
    {% set grant_config = config.get('grants') %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set strategy_macro = silvershot_strategy_dispatch(strategy_name) %}

    {# Zielrelationsexistenz ab diesem Punkt garantiert. Strategie wird per default so gesetzt #}
    {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, True) %}

    
    {# Columns von Target und Temp Relationen prüfen #}
    {% do validate_silvershot_table_structures(target_relation, temp_relation, unique_key, strategy, temporal_cols) %}

    {# Extraktion der snapshot_sql für die Befüllung der Temp Tabelle #}
    {% set staging_select_sql = silver_temp_table(strategy, compiled_sql, target_relation) %}

    {# Bereinigung der Temp Tabelle vor Befüllung #}
    {% do ensure_empty_persistent_temp(temp_relation) %}


    {% set temp_columns= adapter.get_columns_in_relation(temp_relation)
        | rejectattr('name', 'equalto', 'dbt_change_type')
        | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
        | rejectattr('name', 'equalto', 'dbt_unique_key')
        | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
        | list %}

    {% set quoted_temp_columns = [] %}
    {% for column in temp_columns %}
        {% do quoted_temp_columns.append(adapter.quote(column.name)) %}
    {% endfor %}

    {# Ausführung des INSERT INTO SQL Code in der Temp Tabelle #}
    {% set insert_temp_sql = build_fill_persistent_temp_table_sql(
        target=temp_table,
        select_sql=staging_select_sql,
        insert_cols=quoted_temp_columns
    ) %}


    {% set source_columns = adapter.get_columns_in_relation(target_relation)
      | rejectattr('name', 'equalto', 'dbt_change_type')
      | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
      | rejectattr('name', 'equalto', 'dbt_unique_key')
      | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
      | list %}

    {% set quoted_source_columns = [] %}
    {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
    {% endfor %}

    {% set merge_sql = silvershot_merge_sql(
        target = target_relation,
        source = temp_table,
        strategy = strategy,
        insert_cols = quoted_source_columns
      )
    %}


    {% call statement('fill_persistent_stage') %}
        {{ insert_temp_sql }}
    {% endcall %}

    {% call statement('main') %}
        {{ merge_sql }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation, temp_relation]}) }}


{% endmaterialization %}
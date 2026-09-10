-- macros/snapshot_lakehouse_dimension_strategy.sql
-- Custom Strategie zur Änderungsdetektion und Setzung der notwendigen Parameter zum Aufbau der Staging-Tabelle bei Dimensionstabellen

{% macro snapshot_unitemporal_hash_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {# Vorbereitung und Validierung der Parameter für die spätere Übergabe an der Staging-Tabelle-Makro #}
    {% set snapshot_relation = adapter.get_relation(database=node['database']|string, schema=node['schema']|string, identifier=config.get('meta').get('TAB_FKEY')|string) %}
    {% set meta = silvershot_identify_columns(snapshot_relation, config.get('unique_key')) %} --neue Spalte
    {# Menge aller Spalten der Zieltabelle für die Verwendung im load_partial Makro #}
    {% set tgt_tab_cols = adapter.get_columns_in_relation(snapshot_relation) %}
    {% set business_columns = meta['business_cols'] %}
    {% set history_columns = meta['history_cols'] %}
    {% set extra_tech_cols = meta['extra_tech_cols'] %}
    {% set keys_cols = meta['key_cols'] %}
    {% set temporal_cols = config.get('temporal_cols') %}
    {% set on_change_value = silvershot_on_change_value(snapshot_relation, 'unitemporal', temporal_cols) %}
    {% set temp_max = silvershot_active_max_value('unitemporal', snapshot_relation, tgt_tab_cols) %}
    {% set load_filter = var('LOAD_FILTER', none) %}
    {% set unique_key = resolve_unique_key(snapshot_relation, None, tgt_tab_cols) %}
    {% set delete_key = resolve_delete_unique_key(snapshot_relation, None, tgt_tab_cols) %}
    {% set source_unique_key = resolve_unique_key(snapshot_relation, 'DBT_INTERNAL_SOURCE', tgt_tab_cols) %}
    {% set dest_unique_key = resolve_unique_key(snapshot_relation, 'DBT_INTERNAL_DEST', tgt_tab_cols) %}
    {% set inr_col = var('INR_FKEY', none) %}
    {% set clean_table = var('CLEAN_TABLE', 'N') %}
    {% set clean_table_col = temporal_cols.get('von') %}
    {% set clean_table_condition = build_clean_table_condition(clean_table, clean_table_col, on_change_value, 'target_data') %}

    {# Prüft, ob die Spaltenaufteilung stimmt.
       key_cols darf nicht leer sein und die Schlüsselspalten dürfen
       NICHT zusätzlich in business_cols auftauchen, ansonsten iteriert er über eine leere Liste #}
    {{ log("snapshot_unitemporal_strategy | key_cols=" ~ keys_cols|string, info=true) }}
    {{ log("snapshot_unitemporal_strategy | business_cols=" ~ business_columns|string, info=true) }}

    {% if keys_cols is none or (keys_cols | length) == 0 %}
        {% do exceptions.raise_compiler_error(
            "snapshot_unitemporal_strategy: key_cols leer"
        ) %}
    {% endif %}

    {# row_changed bleibt vorerst erhalten: das bitemporale Staging-Makro
       nutzt es weiterhin. Das unitemporale Staging vergleicht stattdessen
       PAYLOAD_HASH__ (siehe build_business_hash). #}
    {% set row_changed %}
        (
            {% for col in business_columns %}
                {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
                or
                (
                    (({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null))
                    or
                    ((not {{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null))
                )
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        )
    {% endset %}

    {{ return({
        "name": "unitemporal_hash",
        "unique_key": unique_key,
        "key_cols": keys_cols,
        "business_cols": business_columns,
        "delete_key": delete_key,
        "source_unique_key": source_unique_key,
        "dest_unique_key": dest_unique_key,
        "row_changed": row_changed,
        "temp_max": temp_max,
        "temporal_cols": temporal_cols,
        "inr_col": inr_col,
        "on_change_value": on_change_value,
        "extra_tech_cols": extra_tech_cols,
        "history_cols": history_columns,
        "cols": tgt_tab_cols,
        "load_filter": load_filter,
        "clean_table": clean_table,
        "clean_table_column": clean_table_col,
        "clean_table_condition": clean_table_condition
    }) }}
{% endmacro %}
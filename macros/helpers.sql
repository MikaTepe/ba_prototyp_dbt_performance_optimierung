{# Makro zur Prüfung der Schemata von Ziel und Temp Tabellen #}
{% macro validate_silvershot_table_structures(
    target_relation, 
    temp_relation, 
    unique_key, 
    strategy, 
    temporal_cols, 
    inst_nr_col='FUSI_QUEL_INST_SCHL',
    aend_zeit_col='AEND_ZEIT',
    change_flag_col='CHANGE_FLAG_COL') %}

    {% if target_relation is none %}
        {% do exception.raise_compiler_error(
            "validate_silvershot_table_structures: target_relation is none"
        ) %}
    {% endif %}

    {% if temp_relation is none %}
        {% do exception.raise_compiler_error(
            "validate_silvershot_table_structures: target_relation is none"
        ) %}
    {% endif %}

    {% if unique_key is string %}
        {% set unique_key = [unique_key] %}
    {% endif %}

    {% if unique_key is none or (unique_key | length) == 0 %}
        {% do exceptions.raise_compiler_error(
            "validate_silvershot_table_structures: unique_key must contain at least one column"
        ) %}
    {% endif %}


    {% set ns = namespace(
        key_cols=[],
        hist_cols=[],
        target_names=[],
        temp_names=[],
        required_target=[],
        required_temp=[],
        target_business_cols=[],
        temp_business_cols=[],
        missing_target=[],
        missing_temp=[],
        missing_temp_business_cols=[],
        extra_temp_business_cols=[],
        errors=[]
    ) %}

    {# Normalisieren von unique key Spaltennamen #}
    {% for col in unique_key %}
        {% do ns.key_cols.append(col | upper) %}
    {% endfor %}

    {% set strategy_norm = strategy.name | lower %}

    {% if strategy_norm in ['uni', 'unitemporal'] %}
        {% set ns.hist_cols = ['VON', 'BIS'] %}
    {% elif strategy_norm in ['bi', 'bitemporal'] %}
        {% set ns.hist_cols = ['VON', 'BIS', 'TECH_ATS', 'TECH_ETS'] %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "validate_silvershot_table_structures: unsupported strategy '" ~ strategy.name ~
            "'. Expected one of: uni, unitemporal, bi, bitemporal"
        ) %}
    {% endif %}

    {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set temp_columns = adapter.get_columns_in_relation(temp_relation) %}

    {% for col in target_columns %}
        {% do ns.target_names.append(col.name | upper) %}
    {% endfor %}

    {% for col in temp_columns %}
        {% do ns.temp_names.append(col.name | upper) %}
    {% endfor %}

    {% for col in ns.key_cols + ns.hist_cols + [inst_nr_col | upper, aend_zeit_col | upper] %}
        {% if col not in ns.required_target %}
            {% do ns.required_target.append(col) %}
        {% endif %}
    {% endfor %}

    {% for col in ns.required_target + [change_flag_col | upper] %}
        {% if col not in ns.required_temp %}
            {% do ns.required_temp.append(col) %}
        {% endif %}
    {% endfor %}

    {% for col in ns.required_target %}
        {% if col not in ns.target_names %}
            {% do ns.missing_target.append(col) %}
        {% endif %}
    {% endfor %}

    {% for col in ns.required_temp %}
        {% if col not in ns.temp_names %}
            {% do ns.missing_temp.append(col) %}
        {% endif %}
    {% endfor %}

    {% for col in ns.target_names %}
        {% if col not in ns.required_target %}
            {% do ns.target_business_cols.append(col) %}
        {% endif %}
    {% endfor %}

    {% for col in ns.temp_names %}
        {% if col not in ns.required_temp %}
            {% do ns.temp_business_cols.append(col) %}
        {% endif %}
    {% endfor %}

    {% if (ns.target_business_cols | length) == 0 %}
        {% do ns.errors.append(
            "Target table must contain at least one business column"
        ) %}
    {% endif %}

    {% for col in ns.target_business_cols %}
        {% if col not in ns.temp_business_cols %}  
            {% do ns.missing_temp_business_cols.append(col) %}
        {% endif %}
    {% endfor %}

    {% for col in ns.temp_business_cols %}
        {% if col not in ns.target_business_cols %}
            {% do ns.extra_temp_business_cols.append(col) %}
        {% endif %}
    {% endfor %}

    {% if (ns.missing_target | length) > 0 %}
        {% do ns.errors.append(
            "Target table is missing required columns: " ~ (ns.missing_target | join (', '))
        ) %}
    {% endif %}

    {% if (ns.missing_temp_business_cols | length) > 0 %}
        {% do ns.errors.append(
            "Temp table is missing target business columns: " ~ (ns.missing_temp_business_cols | join(', '))
        ) %}
    {% endif %}

    {% if (ns.extra_temp_business_cols | length) > 0 %}
        {% do ns.errors.append(
            "Temp table contains business columns not present in target: " ~ (ns.extra_temp_business_cols | join(', '))
        ) %}
    {% endif %}

    {% if (ns.errors | length) > 0 %}
        {% do exceptions.raise_compiler_error(
            "Silverlayer Table Structure validation failed for target '" ~ target_relation ~ "' and temp '" ~ temp_relation ~ "':\n - " ~ (ns.errors | join('\n - ')) 
        ) %}
    {% endif %}

{% endmacro %}


{# Makro zur Bereinigung der Temp Tabelle vor Ausführung der Transformation #}
{% macro ensure_empty_persistent_temp(temp_table) %}

    {% if temp_table is none %}
        {% do exceptions.raise_compiler_error(
            "ensure_empty_persistent_temp: temp_table is none"
        ) %}
    {% endif %}

    {% if not temp_table.is_table %}
        {% do exceptions.raise_compiler_error(
            "ensure_empty_persistent_temp: relation '" ~ temp_table ~ "' is not a table"
        ) %}
    {% endif %}

    {% if execute %}
        {% call statement('truncate_persistent_temp') %}
            truncate table {{ temp_table }}
        {% endcall %}
    {% endif %}

{% endmacro %}




{# Makro zur Filterung der Spalten einer Tabelle. Es wird zwischen 4 Spaltenkategorien unterschieden #}
{% macro silvershot_identify_columns(target_relation, keys_set) %}
    {% set cols = adapter.get_columns_in_relation(target_relation) %}
    {% set col_names = cols | map(attribute='name') | list %}

    {% set upper_names = [] %}
    {% for n in col_names %}{% do upper_names.append(n | upper) %}{% endfor %}
    {% set history_set = ['von','bis','tech_ats','tech_ets'] %}
    {% set extra_tech_set = ['aend_zeit', 'trans_start'] %}

    {% set history_cols = [] %}
    {% set key_cols = [] %}
    {% set extra_tech_cols = [] %}
    {% set business_cols = [] %}

    {% for un in col_names %}
        {% if un in history_set %}
            {% do history_cols.append(un) %}
        {% elif un in keys_set %}
            {% do key_cols.append(un) %}
        {% elif un in extra_tech_set %}
            {% do extra_tech_cols.append(un) %}
        {% else %}
            {% do business_cols.append(un) %}
        {% endif %}
    {% endfor %}

    {{
        return({
            "history_cols": history_cols,
            "business_cols": business_cols,
            "key_cols": key_cols,
            "extra_tech_cols": extra_tech_cols
        })
    }}
{% endmacro %}


{% macro silvershot_temporal_target_type(target_relation, temporal_cols) %}

    {% if target_relation is none %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: target_relation is none"
        ) %}
    {% endif %}

    {% if temporal_cols is none %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: temporal_cols is missing"
        ) %}
    {% endif %}

    {% set von_col = temporal_cols.get('von') %}
    {% set bis_col = temporal_cols.get('bis') %}

    {% if von_col is none or (von_col | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: temporal_cols.von must not be empty"
        ) %}
    {% endif %}

    {% if bis_col is none or (bis_col | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: temporal_cols.bis must not be empty"
        ) %}
    {% endif %}

    {% set cols = adapter.get_columns_in_relation(target_relation) %}
    {% set ns = namespace(von_type=none, bis_type=none) %}

    {% for col in cols %}
        {% if col.name | upper == von_col | upper %}
            {% set ns.von_type = col.data_type | lower %}
        {% endif %}
        {% if col.name | upper == bis_col | upper %}
            {% set ns.bis_type = col.data_type | lower %}
        {% endif %}
    {% endfor %}

    {% if ns.von_type is none %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: column '" ~ von_col ~ "' was not found in target relation " ~ target_relation
        ) %}
    {% endif %}

    {% if ns.bis_type is none %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: column '" ~ bis_col ~ "' was not found in target relation " ~ target_relation
        ) %}
    {% endif %}

    {% set von_is_date = ns.von_type.startswith('date') %}
    {% set von_is_timestamp = ns.von_type.startswith('timestamp') %}
    {% set bis_is_date = ns.bis_type.startswith('date') %}
    {% set bis_is_timestamp = ns.bis_type.startswith('timestamp') %}

    {% if von_is_date and bis_is_date %}
        {{ return('date') }}
    {% elif von_is_timestamp and bis_is_timestamp %}
        {{ return('timestamp') }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "silvershot_temporal_target_type: temporal columns '" ~ von_col ~ "' (" ~ ns.von_type ~ 
            ") and '" ~ bis_col ~ "' (" ~ ns.bis_type ~ ") are not of the same temporal family"
        ) %}
    {% endif %}

{% endmacro %}

{% macro silvershot_cast_temporal_expression(expr_sql, target_type) %}

    {% if expr_sql is none or (expr_sql | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "silvershot_cast_temporal_expression: expr_sql must not be emtpy"
        ) %}
    {% endif %}

    {% if target_type == 'date' %}
        {{ return("cast(" ~ expr_sql ~ " as date)") }}
    {% elif target_type == 'timestamp' %}
        {{ return("cast(" ~ expr_sql ~ " as timestamp(6))") }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "silvershot_cast_temporal_expression: unsupported target_type '" ~ target_type ~ "'"
        ) %}
    {% endif %}

{% endmacro %}


{% macro silvershot_ultimo_value(base_expr_sql, target_type) %}

    {% if base_expr_sql is none or (base_expr_sql | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "silvershot_ultimo_value: base_expr_sql must not be empty"
        ) %}
    {% endif %}

    {% if target_type == 'date' %}
        {{ return("last_day_of_month(date_add('month', -1, cast(" ~ base_expr_sql ~ " as date)))") }}
    {% elif target_type == 'timestamp' %}
        {{ return (
            "cast(last_day_of_month(date_add('month', -1, cast(" ~ base_expr_sql ~ " as date))) as timestamp(6)) + INTERVAL '23:59:59.99999' HOUR TO SECOND"
        ) }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "silvershot_ultimo_value: unsupported target_type '" ~ target_type ~ "'"
        ) %}
    {% endif %}

{% endmacro %}


{% macro silvershot_on_change_value(target_relation, strategy, temporal_cols) %}

    
    {% if strategy is none or (strategy | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "silvershot_on_change_value: stratety must not be empty"
        ) %}
    {% endif %}

    {% set temporal_cfg = config.get('temporal_value') %}
    {% set strategy_norm = strategy | lower | trim %}
    {% set target_type = silvershot_temporal_target_type(target_relation, temporal_cols) %}

    {# default values wenn temporal_value leer ist #}
    {% if temporal_cfg is none or (temporal_cfg | string | trim) == '' %}
        {% if strategy_norm == 'unitemporal' %}
            {% set temporal_cfg = "VAR('BDAT')" %}
        {% elif strategy_norm == 'bitemporal' %}
            {% set temporal_cfg = "CURRENT_TIMESTAMP" %}
        {% else %}
            {% do exceptions.raise_compiler_error(
                "silvershot_on_change_value: unsupported strategy '" ~ strategy ~ 
                "'. Expected unitemporal or bitemporal"
            ) %}
        {% endif %}
    {% endif %}

    {% set token = temporal_cfg | string | trim %}
    {% set token_upper = token | upper %}

    {# 1. CURRENT_TIMESTAMP #}
    {% if token_upper == 'CURRENT_TIMESTAMP' %}
        {{ return(silvershot_cast_temporal_expression("current_timestamp", target_type)) }}
    {# 2. CURRENT_DATE #}
    {% elif token_upper == 'CURRENT_DATE' %}
        {{ return(silvershot_cast_temporal_expression("current_date", target_type)) }}
    {# 3. VAR('BDAT') #}
    {% elif token_upper in ["VAR('BDAT')", 'VAR("BDAT")'] %}
        {% set bdat = var('BDAT') %}
        {% if bdat is none or (bdat | string | trim) == '' %}
            {% do exceptions.raise_compiler_error(
                "silvershot_on_change_value: var('BDAT') is empty or not provided"
            ) %}
        {% endif %}

        {% set bdat_escaped = bdat | string | trim | replace("'", "''") %}

        {% if ' ' in bdat_escaped or 'T' in bdat_escaped %}
            {% set base_expr = "'" ~ bdat_escaped ~ "'" %}
        {% else %}
            {% set base_expr = "'" ~ bdat_escaped ~ "'" %}
        {% endif %}

        {{ return(silvershot_cast_temporal_expression (base_expr, target_type)) }}

    {# 4. CURRTS #}
    {% elif token_upper in ['CURRTS', 'currts'] %}

        {% set currts_value = run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f') %}

        {% set currts_escaped = currts_value | replace("'", "''") %}
        {{ return(silvershot_cast_temporal_expression("'" ~ currts_escaped ~ "'", target_type)) }}

    {# 5. TO_DATE('SOME_VALUE') #}
    {% elif token_upper.startswith('TO_DATE(') and token.endswith(')') %}
        {% set inner = token[8:-1] | trim %}
        {% if inner | length < 2 %}
            {% do exceptions.raise_compiler_error(
                "silvershot_on_change_value: invalid TO_DATE(...) expression '" ~ token ~ "'" 
            ) %}
        {% endif %}

        {% if (inner[0] == "'" and inner [-1] == "'") or (inner[0] == '"' and inner[-1] == '"') %}
            {% set literal_value = inner[1:-1] | trim %}
        {% else %}
            {% do exceptions.raise_compiler_error(
                "silvershot_on_change_value: TO_DATE (...) must contain a qouted literal, got '" ~ token ~ "'"
            ) %}
        {% endif %}

        {% if literal_value == '' %}
            {% do exceptions.raise_compiler_error(
                "silvershot_on_change_value: TO_DATE(...) literal must not be empty"
            ) %}
        {% endif %}

        {% set literal_escaped = literal_value | replace("'", "''") %}
        {{ return(silvershot_cast_temporal_expression("'" ~ literal_escaped ~ "'", target_type)) }}
    
    {# 6. Ultimo '' #}
    {% elif token_upper.startswith('ULTIMO(') and token.endswith(')') %}
        {% set inner = token[7:-1] | trim %}
        {% set ultimo_base_upper = inner | upper %}

        {% if ultimo_base_upper == 'CURRENT_TIMESTAMP' %}
            {% set ultimo_base_expr = "current_timestamp" %}
        {% elif ultimo_base_upper == 'CURRENT_DATE' %}
            {% set ultimo_base_expr = "current_date" %}
        {% elif ultimo_base_upper in ["VAR('BDAT')", 'VAR("BDAT")'] %}
            {% set bdat = var('BDAT') %}
            {% if bdat is none or (bdat | string | trim) == '' %}
                {% do exceptions.raise_compiler_error(
                    "silvershot_on_change_value: var('BDAT') is empty or not provided for ULTIMO"
                ) %}
            {% endif %}
            {% set bdat_escaped = bdat | string | trim | replace("'", "''") %}
            {% set ultimo_base_expr = "'" ~ bdat_escaped ~ "'" %}
        {% elif ultimo_base_upper == 'CURRTS' %}
            {% set currts_value = run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f') %}
            {% set currts_escaped = currts_value | replace("'", "''") %}
            {% set ultimo_base_expr = "'" ~ currts_escaped ~ "''" %}
        {% elif ultimo_base_upper.startswith('TO_DATE(') and inner.endswith(')') %}
            {% set inner_token = inner[8:-1] %}
            {% if (inner_token[0] == "'" and inner_token[-1] == "'") or (inner_token[0] == '"' and inner_token[-1] == '"') %}
                {% set literal_value = inner_token[1:-1] | trim %}
            {% else %}
                {% do exceptions.raise_compiler_error(
                    "silvershot_on_change_value: invalid ULTIMO(TO_DATE(...)) expression '" ~ inner_token ~ "'"
                ) %}
            {% endif %}
            {% if literal_value == '' %}
                {% do exceptions.raise_compiler_error(
                    "silvershot_on_change_value: empty literal in ULTIMO(TO_DATE(...))"
                ) %}
            {% endif %}
            {% set literal_escaped = literal_value | replace ("'", "''") %}
            {% set ultimo_base_expr = "'" ~ literal_escaped ~ "'" %}
        {% elif (inner[0] == "'" and inner[-1] == "'") or (inner[0] == '"' and inner[-1] == '"') %}
            {% set literal_value = inner[1:-1] | trim %}
            {% if literal_value == '' %}
                {% do exceptions.raise_compiler_error(
                    "silvershot_on_change_value: empty literal in ULTIMO(...)"
                ) %}
            {% endif %}
            {% set literal_escaped = literal_value | replace("'", "''") %}
            {% set ultimo_base_expr = "'" ~ literal_escaped ~ "'" %}

        {% else %}
            {% do exceptions.raise_compiler_error(
                "silvershot_on_change_value: unsupported ULTIMO base value '" ~ ultimo_base_token ~ "'"
            ) %}
        {% endif %}

        {{ return(silvershot_ultimo_value(ultimo_base_expr, target_type)) }}

    {% else %}
        {% do exceptions.raise_compiler_error(
            "silvershot_on_change_value: upsupported temporal value '" ~ token ~ "'. "
            ~ "Allowed values are CURRENT_TIMESTAMP, CURRENT_DATE, VAR('BDAT'), CURRTS, TO_DATE('SOME_VALUE')"
        ) %}

    {% endif %}


{% endmacro %}



{% macro resolve_unique_key(target_relation, rel_name, cols, delimiter='م') %}
{% set business_key = config.get('unique_key') %}
{% set business_key_cols = [] %}
{% set business_key_upper = [] %}
{# überprüfen, ob ein custom Logik fürs Business_Key übergeben wird #}
{% if business_key %}
  {# Normalisierung der übergebenen Spalten auf Großbuchstaben #}
  {% for col in business_key %}
    {% do business_key_upper.append(col|upper) %}
  {% endfor %}
  {# Extraktion der Spaltennamen und deren Datentyp aus der Zieltabelle #}
  {% for col in cols %}
    {% if col.name|upper in business_key_upper %}
      {% do business_key_cols.append(col) %}
    {% endif %}
  {% endfor %}
  {# Sicherstellung, dass die übergebenene Spalten wirklich in der Zieltabelle vorhanden sind #}
  {% if business_key.length != business_key_cols.length %}
    {{ exceptions.raise_compiler_error(
        "resolve_business_key: die übergebenen Spalten enthalten Spalten, die in der Zieltabelle nicht vorhanden sind. Bitte die übergebenen Spalten nochmal überprüfen."
    ) }}
  {% endif %}
  {# Die Custom Logik zum Aufbau des Business_keys basteln #}
  {% set pieces = [] %}
  {% for c in business_key_cols %}
    {% if c.data_type is defined and 'VARBINARY' in (c.data_type | upper) %}
      {% if rel_name is not none %}
        {% do pieces.append("coalesce(to_hex(" ~ rel_name ~ "." ~ c.name ~ "), CAST('' AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% else %}
        {% do pieces.append("coalesce(to_hex(" ~ c.name ~ "), CAST('' AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% endif %}
    {% else %}
      {% if rel_name is not none %}
        {% do pieces.append("coalesce(to_hex(to_utf8(CAST(" ~ rel_name ~ "." ~ c.name ~ " AS VARCHAR))), CAST(''AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% else %}
        {% do pieces.append("coalesce(to_hex(to_utf8(CAST(" ~ c.name ~ " AS VARCHAR))), CAST(''AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% set expr = "sha1(to_utf8(concat_ws('" ~ delimiter ~ "', " ~ (pieces | join(', ')) ~ " )))" %}
  {{ return(expr) }}
{% endif %}

{% endmacro %}


{% macro resolve_delete_unique_key(target_relation, rel_name, cols, delimiter='م') %}
{% set business_key = config.get('unique_key') %}
{% set delete_key_cols = [] %}
{% set delete_key_upper = [] %}
{# überprüfen, ob ein custom Logik fürs Business_Key übergeben wird #}
{% if business_key %}
  {# Normalisierung der übergebenen Spalten auf Großbuchstaben #}
  {% for col in business_key %}
    {% do delete_key_upper.append(col|upper) %}
  {% endfor %}
  {% if config.get('temporal_cols').get('von') %}
    {% do delete_key_upper.append(config.get('temporal_cols').get('von') | upper) %}
  {% endif %}

  {% if config.get('temporal_cols').get('bis') %}
    {% do delete_key_upper.append(config.get('temporal_cols').get('bis') | upper) %}
  {% endif %}
  {# Extraktion der Spaltennamen und deren Datentyp aus der Zieltabelle #}
  {% for col in cols %}
    {% if col.name|upper in delete_key_upper %}
      {% do delete_key_cols.append(col) %}
    {% endif %}
  {% endfor %}
  {# Sicherstellung, dass die übergebenene Spalten wirklich in der Zieltabelle vorhanden sind #}
  {% if business_key.length != delete_key_cols.length %}
    {{ exceptions.raise_compiler_error(
        "resolve_business_key: die übergebenen Spalten enthalten Spalten, die in der Zieltabelle nicht vorhanden sind. Bitte die übergebenen Spalten nochmal überprüfen."
    ) }}
  {% endif %}
  {# Die Custom Logik zum Aufbau des Business_keys basteln #}
  {% set pieces = [] %}
  {% for c in delete_key_cols %}
    {% if c.data_type is defined and 'VARBINARY' in (c.data_type | upper) %}
      {% if rel_name is not none %}
        {% do pieces.append("coalesce(to_hex(" ~ rel_name ~ "." ~ c.name ~ "), CAST('' AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% else %}
        {% do pieces.append("coalesce(to_hex(" ~ c.name ~ "), CAST('' AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% endif %}
    {% else %}
      {% if rel_name is not none %}
        {% do pieces.append("coalesce(to_hex(to_utf8(CAST(" ~ rel_name ~ "." ~ c.name ~ " AS VARCHAR))), CAST(''AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% else %}
        {% do pieces.append("coalesce(to_hex(to_utf8(CAST(" ~ c.name ~ " AS VARCHAR))), CAST(''AS VARCHAR)), '" ~ delimiter ~ "'") %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% set expr = "sha1(to_utf8(concat_ws('" ~ delimiter ~ "', " ~ (pieces | join(', ')) ~ " )))" %}
  {{ return(expr) }}
{% endif %}

{% endmacro %}

{% macro silver_merge_key_condition(base_relation, comp_relation, key_cols) %}
    {% set pieces = [] %}
    {% for key in key_cols %}
        {% do pieces.append(base_relation ~ "." ~ key ~ " IS DISTINCT FROM " ~ comp_relation ~ "." ~ key) %}
    {% endfor %}    
    {% set expr = " " ~ (pieces | join('\n AND ')) %}
    {{ return(expr) }}
{% endmacro %}



{% macro silvershot_active_max_value(strategy, target_relation, cols) %}

    {% if strategy is none or (strategy | trim) == '' %}
        {% do exceptions.raise_compiler_error(
            "silvershot_active_max_value: strategy must not be empty"
        ) %}
    {% endif %}

    {% if target_relation is none %}
        {% do exceptions.raise_compiler_error(
            "silvershot_active_max_value: target_relation is none"
        ) %}
    {% endif %}

    {% set strategy_norm = strategy | lower | trim %}

    {% if strategy_norm == 'unitemporal' %}
        {% set active_end_col = 'BIS' %}
    {% elif strategy_norm == 'bitemporal' %}
        {% set active_end_col = 'TECH_ETS' %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "silvershot_active_max_value: unsupported strategy '" ~ strategy ~ 
            "'. Expected unitemporal or bitemporal"
        ) %}
    {% endif %}

    {% set ns = namespace(col_type=none) %}

    {% for col in cols %}
        {% if col.name | upper == active_end_col %}
            {% set ns.col_type = col.data_type | lower %}
        {% endif %}
    {% endfor %}

    {% if ns.col_type is none %}
        {% do exceptions.raise_compiler_error(
            "silvershot_active_max_value: column '" ~ active_end_col ~ 
            "' was not found in target relations " ~ target_relation
        ) %}
    {% endif %}

    {% if ns.col_type.startswith('date') %}
        {{ return("DATE '9999-12-30'") }}
    {% elif ns.col_type.startswith('timestamp') %}
        {{ return("TIMESTAMP '9999-12-30 23:59:59.999'") }}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "silvershot_active_max_value: column '" ~ active_end_col ~ 
            "' has unsupported datatype '" ~ ns.col_type ~ 
            "'. Expected DATE or TIMESTAMP"
        ) %}
    {% endif %}
    

{% endmacro %}



{% macro silver_temp_table_vorlage(strategy, source_sql, target_relation) %}
    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (
        select *,
            {{ strategy.unique_key }} as FKEY
        from {{ target_relation }}
        where
            {% if strategy.name == 'unitemporal' %}
                bis = {{ strategy.temp_max }}
            {% elif strategy.name == 'bitemporal' %}
                tech_ets = {{ strategy.temp_max }}
            {% endif %}
    ),

    insertion_source_data as (

        select
            *,
            {{ strategy.unique_key }} as FKEY,
            {% if strategy.inr_col is not none %}
                {{ strategy.inr_col }} as fusi_quel_inst_schl,
            {% endif %}
            {{ strategy.aend_zeit_val }} as aend_zeit,
            {% if strategy.name == 'unitemporal' %}
                {{ strategy.temporal_value }} as von,
            {% elif strategy.name == 'bitemporal' %}
                {{ strategy.temporal_value }} as tech_ats,
            {% endif %}
            {% if strategy.name == 'unitemporal' %}
                {{ strategy.temp_max }} as bis
            {% elif strategy.name == 'bitemporal' %}
                {{ strategy.temp_max }} as tech_ets
            {% endif %}

        from snapshot_query

    ),

    insertions as (
        select
            'insert' as change_flag_col,
            source_data.*
        
        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.FKEY = source_data.FKEY
        where snapshotted_data.FKEY is null
            or (
                snapshotted_data.fkey is not null
                and (
                    {{ strategy.row_changed }}
                )
            )
    ),

    updates as (
        
        select
            'update' as change_flag_col,
            source_data.*

        from insertion_source_data as source_data
        join snapshotted_data on snapshotted_data.FKEY = source_data.FKEY
        where (
            {{ strategy.row_changed }}
        )

    ),

    invalidations as (

        select
            'invalidate' as change_flag_col,
            snapshotted_data.*
        from snapshotted_data
        left join insertions_source_data as source_data
            on snapshotted_data.FKEY = source_data.FKEY
        where source_data.FKEY is null

    )

    select * from insertions
    union all
    select * from updates
    union all
    select * from invalidations

{% endmacro %}


{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{% endmacro %}






{% macro resolve_load_filter_condition(relation_alias=none) %}

    {% set load_filter = var('LOAD_FILTER', none) %}

    {{ log("load_filter: " ~ load_filter|string, info=true) }}

    {%- if load_filter is none or load_filter | trim == '' -%}
        {% do return(none) %}
    {%- endif -%}

    {%- set filter_text = load_filter
        | replace('“', '"')
        | replace('”', '"')
        | replace('‘', "'")
        | replace('’', "'")
        | trim
    -%}

    {%- set lower_filter = filter_text | lower -%}

    {%- if ';' in filter_text
        or '--' in filter_text
        or '/*' in filter_text
        or '*/' in filter_text -%}

        {{ exceptions.raise_compiler_error(
            "Invalid load_filter: semicolons and SQL comments are not allowed."
        ) }}

    {%- endif -%}

    {%- set forbidden_keywords = [
        ' insert ',
        ' update ',
        ' delete ',
        ' merge ',
        ' drop ',
        ' alter ',
        ' create ',
        ' truncate ',
        ' grant ',
        ' revoke '
    ] -%}

    {%- for keyword in forbidden_keywords -%}
        {%- if keyword in (' ' ~ lower_filter ~ ' ') -%}
            {{ exceptions.raise_compiler_error(
                "Invalid load_filter: forbidden keyword found: " ~ keyword | trim
            ) }}
        {%- endif -%}
    {%- endfor -%}

    
    {%- if relation_alias is not none and relation_alias | trim != '' -%}
        {%- set filter_text = filter_text | replace('__alias__', relation_alias) -%}
        {%- set filter_text = filter_text | replace('{alias}', relation_alias) -%}
    {%- endif -%}

    {{ log("filter_text: " ~ filter_text|string, info=true) }}


    {% do return(filter_text) %}

{% endmacro %}

{% macro build_business_hash(business_cols, rel_name) %}

    xxhash64(
        to_utf8(
            concat_ws('م', 
                {% for col in business_cols %}
                    coalesce(cast({{ rel_name }}.{{ col }} as varchar),'<NULL>')
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        )
    )
{% endmacro %}
{% macro silver_transactional_temporal_column() %}

  {% set temporal_column = config.get('temporal_column', config.get('temporal_col', 'TRANS_DTM')) %}

  {% if temporal_column is none or temporal_column | string | trim == '' %}
    {{ exceptions.raise_compiler_error(
        "silver_transactional config temporal_column must not be empty. Default is TRANS_DTM."
    ) }}
  {% endif %}

  {{ return(temporal_column | string | trim) }}

{% endmacro %}


{% macro silver_transactional__cast_temporal_expr(expr_sql, data_type='date') %}
  {{ return("cast(" ~ expr_sql ~ " as " ~ data_type ~ ")") }}
{% endmacro %}


{% macro silver_transactional__strip_outer_quotes(value) %}

  {% set v = value | string | trim %}

  {% if v | length >= 2 %}
    {% set first = v[0:1] %}
    {% set last = v[-1:] %}

    {% if first == "'" and last == "'" %}
      {{ return(v[1:-1] | trim) }}
    {% elif first == '"' and last == '"' %}
      {{ return(v[1:-1] | trim) }}
    {% endif %}
  {% endif %}

  {{ return(v) }}

{% endmacro %}



{% macro silver_transactional__is_iso_date(value) %}

  {% set v = value | string | trim %}

  {% if v | length != 10 %}
    {{ return(false) }}
  {% endif %}

  {% if v[4:5] != '-' or v[7:8] != '-' %}
    {{ return(false) }}
  {% endif %}

  {% set year = v[0:4] %}
  {% set month = v[5:7] %}
  {% set day = v[8:10] %}

  {% set digits = '0123456789' %}

  {% for c in year %}
    {% if c not in digits %}
      {{ return(false) }}
    {% endif %}
  {% endfor %}

  {% for c in month %}
    {% if c not in digits %}
      {{ return(false) }}
    {% endif %}
  {% endfor %}

  {% for c in day %}
    {% if c not in digits %}
      {{ return(false) }}
    {% endif %}
  {% endfor %}

  {% if month | int < 1 or month | int > 12 %}
    {{ return(false) }}
  {% endif %}

  {% if day | int < 1 or day | int > 31 %}
    {{ return(false) }}
  {% endif %}

  {{ return(true) }}

{% endmacro %}



{% macro silver_transactional__function_inner(value, function_name) %}

  {% set v = value | string | trim %}
  {% set upper_v = v | upper %}
  {% set upper_fn = function_name | upper %}
  {% set fn_len = upper_fn | length %}

  {% if upper_v[0:fn_len] != upper_fn %}
    {{ return(none) }}
  {% endif %}

  {% set rest = v[fn_len:] | trim %}

  {% if rest[0:1] != '(' or rest[-1:] != ')' %}
    {{ return(none) }}
  {% endif %}

  {{ return(rest[1:-1] | trim) }}

{% endmacro %}



{% macro silver_transactional__date_from_date_expression(value) %}

  {% set v = value | string | trim %}
  {% set upper_v = v | upper %}

  {# Supports DATE("2026-01-01") and DATE ('2026-01-01') #}
  {% set inner = silver_transactional__function_inner(v, 'DATE') %}

  {% if inner is not none %}
    {% set date_value = silver_transactional__strip_outer_quotes(inner) %}

    {% if silver_transactional__is_iso_date(date_value) %}
      {{ return(date_value) }}
    {% endif %}

    {{ return(none) }}
  {% endif %}

  {# Supports DATE "2026-01-01" and DATE '2026-01-01' #}
  {% if upper_v[0:4] == 'DATE' %}
    {% set rest = v[4:] | trim %}
    {% set date_value = silver_transactional__strip_outer_quotes(rest) %}

    {% if silver_transactional__is_iso_date(date_value) %}
      {{ return(date_value) }}
    {% endif %}
  {% endif %}

  {{ return(none) }}

{% endmacro %}




{% macro silver_transactional__date_from_to_date_expression(value) %}

  {% set inner = silver_transactional__function_inner(value, 'TO_DATE') %}

  {% if inner is none %}
    {{ return(none) }}
  {% endif %}

  {% set date_value = silver_transactional__strip_outer_quotes(inner) %}

  {% if silver_transactional__is_iso_date(date_value) %}
    {{ return(date_value) }}
  {% endif %}

  {{ return(none) }}

{% endmacro %}




{% macro silver_transactional__ultimo_expr(value) %}

  {% set inner = silver_transactional__function_inner(value, 'ULTIMO') %}

  {% if inner is none %}
    {{ return(none) }}
  {% endif %}

  {% set ultimo_base_date = silver_transactional__date_from_to_date_expression(inner) %}

  {% if ultimo_base_date is none %}
    {{ exceptions.raise_compiler_error(
        "Invalid ULTIMO expression for silver_transactional. Expected ULTIMO(TO_DATE(\"YYYY-MM-DD\")). Got: " ~ value
    ) }}
  {% endif %}

  {{ return("date_add('day', -1, date_trunc('month', date '" ~ ultimo_base_date ~ "'))") }}

{% endmacro %}





{% macro silver_transactional_temporal_value(data_type='date') %}

  {% set temporal_value = config.get('temporal_value', 'VAR("BDAT")') %}

  {% if temporal_value is none %}
    {{ exceptions.raise_compiler_error(
        "silver_transactional requires config temporal_value. Allowed examples: VAR(\"BDAT\"), DATE(\"2026-01-01\"), ULTIMO(TO_DATE(\"2026-02-03\")), CURRENT_TIMESTAMP, CURR_TS, CURRENT_DATE."
    ) }}
  {% endif %}

  {% set raw_value = temporal_value | string | trim %}
  {% set upper_value = raw_value | upper %}

  {# VAR("BDAT"), VAR('BDAT'), VAR(BDAT), BDAT, VAR_BDAT #}
  {% set var_inner = silver_transactional__function_inner(raw_value, 'VAR') %}

  {% if upper_value in ['BDAT', 'VAR_BDAT'] or (var_inner is not none and silver_transactional__strip_outer_quotes(var_inner) | upper == 'BDAT') %}

    {% set bdat = var('BDAT', none) %}

    {% if bdat is none %}
      {{ exceptions.raise_compiler_error(
          "silver_transactional temporal_value='" ~ raw_value ~ "' requires var('BDAT') to be passed."
      ) }}
    {% endif %}

    {% set bdat_string = silver_transactional__strip_outer_quotes(bdat) %}

    {% if not silver_transactional__is_iso_date(bdat_string) %}
      {{ exceptions.raise_compiler_error(
          "silver_transactional var('BDAT') must be an ISO date in format YYYY-MM-DD. Got: " ~ bdat_string
      ) }}
    {% endif %}

    {{ return(silver_transactional__cast_temporal_expr("date '" ~ bdat_string ~ "'", data_type)) }}

  {% endif %}

  {# DATE("2026-01-01"), DATE '2026-01-01' #}
  {% set static_date = silver_transactional__date_from_date_expression(raw_value) %}

  {% if static_date is not none %}
    {{ return(silver_transactional__cast_temporal_expr("date '" ~ static_date ~ "'", data_type)) }}
  {% endif %}

  {# Direct static ISO date: 2026-01-01, '2026-01-01', "2026-01-01" #}
  {% set raw_without_quotes = silver_transactional__strip_outer_quotes(raw_value) %}

  {% if silver_transactional__is_iso_date(raw_without_quotes) %}
    {{ return(silver_transactional__cast_temporal_expr("date '" ~ raw_without_quotes ~ "'", data_type)) }}
  {% endif %}

  {# ULTIMO(TO_DATE("2026-02-03")) #}
  {% set ultimo_sql = silver_transactional__ultimo_expr(raw_value) %}

  {% if ultimo_sql is not none %}
    {{ return(silver_transactional__cast_temporal_expr(ultimo_sql, data_type)) }}
  {% endif %}

  {# CURRENT_TIMESTAMP #}
  {% if upper_value in ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP()'] %}
    {{ return(silver_transactional__cast_temporal_expr("current_timestamp", data_type)) }}
  {% endif %}

  {# CURR_TS = stable dbt run-start timestamp #}
  {% if upper_value in ['CURR_TS', 'CURRENT_TS', 'RUN_STARTED_AT'] %}

    {% set run_started_at_string = run_started_at.isoformat() %}

    {{ return(silver_transactional__cast_temporal_expr(
        "from_iso8601_timestamp('" ~ run_started_at_string ~ "')",
        data_type
    )) }}

  {% endif %}

  {# CURRENT_DATE #}
  {% if upper_value in ['CURRENT_DATE', 'CURRENT_DATE()'] %}
    {{ return(silver_transactional__cast_temporal_expr("current_date", data_type)) }}
  {% endif %}

  {{ exceptions.raise_compiler_error(
      "Invalid temporal_value for silver_transactional. Allowed examples: " ~
      "VAR(\"BDAT\"), DATE(\"2026-01-01\"), ULTIMO(TO_DATE(\"2026-02-03\")), " ~
      "CURRENT_TIMESTAMP, CURR_TS, CURRENT_DATE. Got: " ~ raw_value
  ) }}

{% endmacro %}




{% macro silver_transactional__get_column_case_insensitive(relation, column_name, relation_role='relation') %}

  {% if not execute %}
    {{ return(none) }}
  {% endif %}

  {% set cols = adapter.get_columns_in_relation(relation) %}
  {% set matches = [] %}

  {% for col in cols %}
    {% if col.name | lower == column_name | lower %}
      {% do matches.append(col) %}
    {% endif %}
  {% endfor %}

  {% if matches | length == 0 %}
    {{ exceptions.raise_compiler_error(
        "silver_transactional validation failed: column '" ~ column_name ~ "' does not exist in " ~ relation_role ~ " " ~ relation ~ "."
    ) }}
  {% endif %}

  {{ return(matches[0]) }}

{% endmacro %}


{% macro silver_transactional__validate_temp_columns(temp_relation, dest_columns, temporal_column_name, ignore_cols=[]) %}

  {% if not execute %}
    {{ return(true) }}
  {% endif %}

  {% set temp_cols = adapter.get_columns_in_relation(temp_relation) %}
  {% set temp_col_names_lower = [] %}
  {% set ignore_cols_lower = [] %}

  {% for ignore_col in ignore_cols %}
    {% do ignore_cols_lower.append(ignore_col | lower) %}
  {% endfor %}

  {% for col in temp_cols %}
    {% do temp_col_names_lower.append(col.name | lower) %}
  {% endfor %}

  {% set missing = [] %}

  {% for col in dest_columns %}
    {% if col.name | lower != temporal_column_name | lower %}
      {% if col.name | lower not in ignore_cols_lower %}
        {% if col.name | lower not in temp_col_names_lower %}
          {% do missing.append(col.name) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% if missing | length > 0 %}
    {{ exceptions.raise_compiler_error(
        "silver_transactional validation failed: temp relation " ~ temp_relation ~
        " is missing target columns: " ~ (missing | join(', ')) ~
        ". The only target column allowed to be missing from the model SQL is the configured temporal_column '" ~ temporal_column_name ~ "'."
    ) }}
  {% endif %}

  {{ return(true) }}

{% endmacro %}
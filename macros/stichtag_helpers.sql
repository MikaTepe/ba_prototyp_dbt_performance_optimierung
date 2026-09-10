{% macro silver_stichtag_temporal_column() %}

  {% set temporal_column = config.get('temporal_column', config.get('temporal_col', 'STICHTAG')) %}

  {% if temporal_column is none or temporal_column | string | trim == '' %}
    {{ exceptions.raise_compiler_error(
        "silver_stichtag config temporal_column must not be empty. Default is STICHTAG."
    ) }}
  {% endif %}

  {{ return(temporal_column | string | trim) }}

{% endmacro %}

{% macro silver_stichtag_temporal_value(data_type='date') %}

    {% set temporal_value = config.get('temporal_value', 'BDAT') %}

    {% if temporal_value is none %}
        {{ exceptions.raise_compiler_error(
            "silver_stichtag requires config temporal_value. Allowed values: 'BDAT', 'CURRENT_DATE' or a static ISO date like '2026-07-07'."
        ) }}
    {% endif %}

    {% set raw_value = temporal_value | string | trim %}
    {% set upper_value = raw_value | upper %}

    {% if upper_value in ['BDAT', 'VAR_BDAT', 'VAR("BDAT")'] %}

        {% set bdat = var('BDAT', none) %}

        {% if bdat is none %}
            {{ exceptions.raise_compiler_error(
                "silver_stichtag temporal_value='BDAT' requires var('BDAT') to be passed."
            ) }}
        {% endif %}

        {% set bdat_string = bdat | string | trim %}

        {% if modules.re.match('^\d{4}-\d{2}-\d{2}$', bdat_string) is none %}
            {{ exceptions.raise_compiler_error(
                "silver_stichtag var('BDAT') must be an ISO date in format YYYY-MM-DD. Got: " ~ bdat_string
            ) }}
        {% endif %}

        {{ return("cast('" ~ bdat_string ~ "' as " ~ data_type ~ ")") }}

    {% elif upper_value in ['CURRENT_DATE', 'CURRENT_DATE()'] %}

        {{ return("cast(current_date as " ~ data_type ~ ")") }}

    {% else %}

        {% set static_value = raw_value | replace ("'", "") | trim %}

        {% if modules.re.match('^\d{4}-\d{2}-\d{2}$', static_value) is none %}
            {{ exceptions.raise_compiler_error(
                "Invalid temporal_value for silver_stichtag. Allowed values: 'BDAT', 'CURRENT_DATE', or a static ISO date like '2026-07-07'. Got: " ~ raw_value
            ) }}
        {% endif %}

        {{ return("cast('" ~ static_value ~ "' as " ~ data_type ~ ")") }}

    {% endif %}

{% endmacro %}

{% macro silver_stichtag__get_column_case_insensitive(relation, column_name, relation_role='relation') %}

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
            "silver_stichtag validation failed: column '" ~ column_name ~ "' does not exist in " ~ relation_role ~ " " ~ relation ~ "."
        ) }}
    {% endif %}

    {{ return(matches[0]) }}

{% endmacro %}




{% macro silver_stichtag__validate_temp_columns(temp_relation, dest_columns, temporal_column_name, ignore_cols=[]) %}

  {% if not execute %}
    {{ return(true) }}
  {% endif %}

  {% set temp_cols = adapter.get_columns_in_relation(temp_relation) %}
  {% set temp_col_names_lower = [] %}

  {% for col in temp_cols %}
    {% do temp_col_names_lower.append(col.name | lower) %}
  {% endfor %}

  {% set missing = [] %}

  {% for col in dest_columns %}
    {% if col.name | lower != temporal_column_name | lower %}
      {% if col.name | lower not in ignore_cols %}
        {% if col.name | lower not in temp_col_names_lower %}
            {% do missing.append(col.name) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% if missing | length > 0 %}
    {{ exceptions.raise_compiler_error(
        "silver_stichtag validation failed: temp relation " ~ temp_relation ~
        " is missing target columns: " ~ (missing | join(', ')) ~
        ". The only target column allowed to be missing from the model SQL is the configured temporal_column '" ~ temporal_column_name ~ "'."
    ) }}
  {% endif %}

  {{ return(true) }}

{% endmacro %}


{% macro silver_temporal_check_duplicates(
    target_relation,
    temp_relation,
    unique_key,
    temporal_column_sql,
    temporal_value_sql,
    clean_table,
    load_filter_sql='true',
    strategy_name='silver_temporal'
) %}

  {% if not execute %}
    {{ return(true) }}
  {% endif %}

  {% if unique_key is string %}
    {% set key_cols = [unique_key] %}
  {% else %}
    {% set key_cols = unique_key %}
  {% endif %}

  {% if key_cols is none or key_cols | length == 0 %}
    {{ exceptions.raise_compiler_error(
        strategy_name ~ " duplicate detection requires unique_key."
    ) }}
  {% endif %}

  {% set source_key_exprs = [] %}
  {% set target_key_exprs = [] %}

  {% for key_col in key_cols %}
    {% do source_key_exprs.append("s." ~ adapter.quote(key_col)) %}
    {% do target_key_exprs.append("t." ~ adapter.quote(key_col)) %}
  {% endfor %}

  {% set source_key_csv = source_key_exprs | join(', ') %}

  {% set join_predicates = [] %}
  {% for key_col in key_cols %}
    {% do join_predicates.append(
        "t." ~ adapter.quote(key_col) ~ " = s." ~ adapter.quote(key_col)
    ) %}
  {% endfor %}

  {% set join_predicate_sql = join_predicates | join(' and ') %}

  {# 1. Incoming duplicates inside the model result #}
  {% set incoming_duplicate_sql %}
    select count(*) as bad_count
    from (
      select
        {{ source_key_csv }},
        count(*) as cnt
      from {{ temp_relation }} s
      group by {{ source_key_csv }}
      having count(*) > 1
    ) bad
  {% endset %}

  {% set incoming_duplicates = run_query(incoming_duplicate_sql) %}

  {% if incoming_duplicates is not none and incoming_duplicates.rows[0][0] > 0 %}
    {{ exceptions.raise_compiler_error(
        strategy_name ~ " duplicate detection failed: incoming model result contains duplicate rows for unique_key. " ~
        "Bad key groups count=" ~ incoming_duplicates.rows[0][0]
    ) }}
  {% endif %}

  {#
    2. Target collision only matters for CLEAN_TABLE=N.
       For S/F, the relevant target scope is deleted before insert.
  #}
  {% if clean_table == 'N' %}

    {% set target_collision_sql %}
      select count(*) as bad_count
      from (
        select
          {{ source_key_csv }}
        from {{ temp_relation }} s
        inner join {{ target_relation }} t
          on {{ join_predicate_sql }}
        where t.{{ temporal_column_sql }} = {{ temporal_value_sql }}
        {% if load_filter_sql %}
          and ({{ load_filter_sql }})
        {% endif %}
        group by {{ source_key_csv }}
      ) bad
    {% endset %}

    {% set target_collisions = run_query(target_collision_sql) %}

    {% if target_collisions is not none and target_collisions.rows[0][0] > 0 %}
      {{ exceptions.raise_compiler_error(
          strategy_name ~ " duplicate detection failed: target already contains rows for the same unique_key and temporal value. " ~
          "Use CLEAN_TABLE='S' to replace the temporal slice, or load only new keys. " ~
          "Bad key groups count=" ~ target_collisions.rows[0][0]
      ) }}
    {% endif %}

  {% endif %}

  {{ return(true) }}

{% endmacro %}
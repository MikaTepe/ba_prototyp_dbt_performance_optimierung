

{% macro ensure_dropped_staging_table(staging_relation) %}
    {% if execute %}

        {{ log("Dropping existing staging table if it exists: " ~ staging_relation, info=True) }}

        {% call statement('drop_existing_staging_table', auto_begin=False) %}
            drop view if exists {{ staging_relation }}
        {% endcall %}

    {% endif %}

    {% do return(staging_relation) %}
{% endmacro %}

{% macro build_clean_table_condition(clean_table, clean_table_column, on_change_value, target_alias='target_data') %}

    {% if clean_table == 'S' %}

        {% if strategy.clean_table_column is defined and strategy.clean_table_column is not none %}
            {% do return(target_alias ~ '.' ~ strategy.clean_table_column ~ ' = ' ~ strategy.on_change_value) %}
        {% else %}
            {% do return(target_alias ~ '.frm = ' ~ strategy.on_change_value) %}
        {% endif %}

    {% else %}

        {% do return(none) %}

    {% endif %}

{% endmacro %}



{% macro ultimo(token, target_type) %}
    
    {% set inner = token | trim %}
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

{% endmacro %}




{% macro check_duplicate_entries(temp_relation, key_expr, delete_row_expr, flag_col='dbt_change_type') %}

  {% if not execute %}
      {{ return(true) }}
  {% endif %}

  {% if delete_row_expr is none %}
    {% set delete_row_expr = key_expr %}
  {% endif %}

  {# 
    The snapshot staging relation may be a view.
    To avoid re-evaluating that view for every validation query, we materialize
    only the columns needed for validation into a narrow helper table.
  #}

  {% set validation_relation = temp_relation.incorporate(
      path={"identifier": temp_relation.identifier ~ "__dupchk"}
  ) %}

  {# Clean up a possible leftover validation table/view from a previous failed run #}
  {% set existing_validation_relation = adapter.get_relation(
      database=validation_relation.database,
      schema=validation_relation.schema,
      identifier=validation_relation.identifier
  ) %}

  {% if existing_validation_relation is not none %}
    {% do drop_relation(existing_validation_relation) %}
  {% endif %}

  {% call statement('build_snapshot_duplicate_validation_relation', auto_begin=False) %}
    create table {{ validation_relation }} as
    select
        {{ key_expr }} as business_key,
        {{ delete_row_expr }} as delete_row_key,
        {{ flag_col }} as flag
    from {{ temp_relation }}
  {% endcall %}

  {#
    Run all checks against the narrow helper table.

    This avoids:
      - scanning the staging view repeatedly
      - fetching all invalid/duplicate keys into dbt
      - running five separate run_query calls

    Instead, every check returns only:
      check_name, bad_count
  #}

  {% set validation_sql %}
    with invalid_flags as (
      select
          'invalid_flag_values' as check_name,
          count(*) as bad_count
      from (
        select flag
        from {{ validation_relation }}
        where flag not in ('insert', 'update', 'invalidate', 'delete')
           or flag is null
        group by flag
      ) invalid_flag_groups
    ),

    duplicate_inserts as (
      select
          'duplicate_insert_rows_per_business_key' as check_name,
          count(*) as bad_count
      from (
        select business_key
        from {{ validation_relation }}
        where flag = 'insert'
        group by business_key
        having count(*) > 1
      ) bad_insert_keys
    ),

    duplicate_history_actions as (
      select
          'duplicate_update_or_invalidate_rows_per_business_key' as check_name,
          count(*) as bad_count
      from (
        select business_key, flag
        from {{ validation_relation }}
        where flag in ('update', 'invalidate')
        group by business_key, flag
        having count(*) > 1
      ) bad_history_groups
    ),

    duplicate_deletes as (
      select
          'duplicate_delete_rows_per_target_row_identity' as check_name,
          count(*) as bad_count
      from (
        select delete_row_key
        from {{ validation_relation }}
        where flag = 'delete'
        group by delete_row_key
        having count(*) > 1
      ) bad_delete_keys
    ),

    bad_patterns as (
      select
          'invalid_flag_pattern_per_business_key' as check_name,
          count(*) as bad_count
      from (
        select
            business_key,
            max(case when flag = 'insert' then 1 else 0 end) as has_i,
            max(case when flag = 'update' then 1 else 0 end) as has_u,
            max(case when flag = 'invalidate' then 1 else 0 end) as has_inv,
            max(case when flag = 'delete' then 1 else 0 end) as has_d,
            count(*) as row_cnt
        from {{ validation_relation }}
        group by business_key
      ) flags
      where not (
        (has_i = 1 and has_u = 0 and has_inv = 0 and has_d = 0)  -- {I}
        or
        (has_i = 1 and has_u = 0 and has_inv = 0 and has_d = 1)  -- {I, D}
        or
        (has_i = 0 and has_u = 0 and has_inv = 0 and has_d = 1)  -- {D}
        or
        (has_i = 0 and has_u = 0 and has_inv = 1 and has_d = 0)  -- {INV}
        or
        (has_i = 1 and has_u = 1 and has_inv = 0 and has_d = 0)  -- {U, I}
      )
    )

    select check_name, bad_count
    from (
      select * from invalid_flags
      union all
      select * from duplicate_inserts
      union all
      select * from duplicate_history_actions
      union all
      select * from duplicate_deletes
      union all
      select * from bad_patterns
    ) checks
    where bad_count > 0
  {% endset %}

  {% set validation_result = run_query(validation_sql) %}

  {# Clean up the narrow validation helper relation before raising an error #}
  {% set existing_validation_relation_after_check = adapter.get_relation(
      database=validation_relation.database,
      schema=validation_relation.schema,
      identifier=validation_relation.identifier
  ) %}

  {% if existing_validation_relation_after_check is not none %}
    {% do drop_relation(existing_validation_relation_after_check) %}
  {% endif %}

  {% if validation_result is not none and validation_result.rows | length > 0 %}

    {% set error_parts = [] %}

    {% for row in validation_result.rows %}
      {% do error_parts.append(row[0] ~ '=' ~ row[1]) %}
    {% endfor %}

    {{ exceptions.raise_compiler_error(
        "TEMP-Validation failed: " ~ (error_parts | join('; '))
    ) }}

  {% endif %}

  {{ return(true) }}

{% endmacro %}
{% macro snapshot_staging_table_silver_unitemporal(strategy, source_sql, target_relation) %}

    {% set clean_table = strategy.get('clean_table') %}
    {% set clean_table_condition = strategy.get('clean_table_condition') %}
    {% set load_filter_source_condition = resolve_load_filter_condition('source_data') %}
    {{ log("load_filter_source_condition : " ~ load_filter_source_condition|string, info=true) }}
    {% set load_filter_target_condition = resolve_load_filter_condition('target_base') %}

    with base_snapshot_query as (

        {{ source_sql }}

    ),

    snapshot_query as (
        select *
        from base_snapshot_query as source_data
        {% if load_filter_source_condition is not none %}
            where {{ load_filter_source_condition }}
        {% endif %}
    ),

    hard_delete_candidates as (
        select *,
            {{ strategy.unique_key }} as FKEY
        from {{ target_relation }} as target_data

        {% if clean_table == 'F' %}
            where 1 = 1
        {% elif clean_table == 'S' %}
            where
                {% if clean_table_condition is not none %}
                    {{ clean_table_condition }}
                {% else %}
                    target_data.{{ strategy.temporal_cols.get('von') }} = {{ strategy.on_change_value }}
                {% endif %}
        {% else %}
            where 1 = 0
        {% endif %}
    ),

    target_base as (
        select *,
            {{ strategy.unique_key }} as FKEY
        from {{ target_relation }} as target_data

        
        {% if clean_table == 'F' %}
            where 1 = 0
        {% elif clean_table == 'S' %}
            where not (
                {% if clean_table_condition is not none %}
                    {{ clean_table_condition }}
                {% else %}
                    target_data.{{ strategy.temporal_cols.get('von') }} = {{ strategy.on_change_value }}
                {% endif %}
            )
        {% endif %}      
    ),

    snapshotted_data as (
        select *
        from target_base
        where
            {{ strategy.temporal_cols.get('bis') }} = {{ strategy.temp_max }}
        {% if load_filter_target_condition is not none %}
            and {{ load_filter_target_condition }}
        {% endif %}  
    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as FKEY,
            CURRENT_TIMESTAMP as aend_zeit,
            {{ strategy.on_change_value }} as {{ strategy.temporal_cols.get('von') }},
            {{ strategy.temp_max }} as {{ strategy.temporal_cols.get('bis') }}

        from snapshot_query

    ),

    insertions as (
        select
            'insert' as dbt_change_type,
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
            'update' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        join snapshotted_data on snapshotted_data.FKEY = source_data.FKEY
        where (
            {{ strategy.row_changed }}
        )

    ),

    invalidations as (

        select
            'invalidate' as dbt_change_type,
            snapshotted_data.*
        from snapshotted_data
        left join insertions_source_data as source_data
            on snapshotted_data.FKEY = source_data.FKEY
        where source_data.FKEY is null

    ),

    deletions as (
        select
            'delete' as dbt_change_type,
            hard_delete_candidates.*

        from hard_delete_candidates
    )

    select 
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    , dbt_change_type
    from insertions
    union all
    select 
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    , dbt_change_type
    from updates
    union all
    select 
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    , dbt_change_type
    from invalidations
    union all
    select
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
        , dbt_change_type
    from deletions

{% endmacro %}
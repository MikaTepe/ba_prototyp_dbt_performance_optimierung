{% macro silver_unitemporal_temp_table(strategy, source_sql, target_relation) %}
    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (
        select *,
            {{ strategy.unique_key }} as FKEY
        from {{ target_relation }}
        where
            bis = {{ strategy.temp_max }}
    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as FKEY,
            CURRENT_TIMESTAMP as aend_zeit,
            {{ strategy.on_change_value }} as von,
            {{ strategy.temp_max }} as bis

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

        from insertions_source_data as source_data
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

    select 
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    , change_flag_col
    from insertions
    union all
    select 
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    , change_flag_col
    from updates
    union all
    select 
        {%- for c in strategy.cols -%}
            {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    , change_flag_col
    from invalidations

{% endmacro %}
{% macro snapshot_staging_table_silver_unitemporal_hash(strategy, source_sql, target_relation) %}

    {% set clean_table = strategy.get('clean_table') %}
    {% set clean_table_condition = strategy.get('clean_table_condition') %}
    {% set load_filter_source_condition = resolve_load_filter_condition('source_data') %}
    {% set load_filter_target_condition = resolve_load_filter_condition('target_base') %}
    {% set payload_hash_source = build_business_hash(strategy.business_cols, 'source_data') %}
    {% set payload_hash_target = build_business_hash(strategy.business_cols, 'target_base') %}

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

    {# Dupe-Check, 1:1 übernommen aus der mail #}
    {# Liefert genau eine Zeile mit OK__ = true, oder bricht per fail ab. #}
    dup_check as (
        select
            case when count(*) > 0
                 then fail('Duplikate im Quellmodell je Business Key')
                 else true
            end as OK__
        from (
            select 1
            from snapshot_query as source_data
            group by
                {%- for col in strategy.key_cols %}
                    source_data.{{ col }}{% if not loop.last %},{% endif %}
                {%- endfor %}
            having count(*) > 1
        ) duplicate_keys
    ),

    {# Compare, vergleicht die Payload-Hashes über den PAYLOAD_KEY__ miteinander #}
    insertions_source_data as (
        select
            {%- for col in strategy.key_cols %}
                source_data.{{ col }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            , {%- for col in strategy.business_cols %}
                source_data.{{ col }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            , {{ strategy.unique_key }} as PAYLOAD_KEY__
            , {{ payload_hash_source }} as PAYLOAD_HASH__
            , CURRENT_TIMESTAMP as aend_zeit
            , {{ strategy.on_change_value }} as {{ strategy.temporal_cols.get('von') }}
            , {{ strategy.temp_max }} as {{ strategy.temporal_cols.get('bis') }}

        from snapshot_query as source_data
        cross join dup_check dc
        where dc.OK__
    ),

    {# Aus snapshot_staging_table_silver_unitemporal.sql übernommen; sammelt alle Sätze die gelöscht werden sollen #}
    hard_delete_candidates as (
        select *,
            {{ strategy.unique_key }} as PAYLOAD_KEY__
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
    {# Aus snapshot_staging_table_silver_unitemporal.sql übernommen; Komplement #}
    target_base as (
        select *
        from {{ target_relation }} as target_data

        {% if clean_table == 'F' %}
            where 1 = 0
        {% elif clean_table == 'N' %}
            where 1 = 1
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

    {# Größtenteils aus Bestand, prüft gegen den kompletten Zielbestand, nicht nur gegen
       die aktiven Sätze, deshalb target_base und nicht snapshotted_data. #}
    assert_target as (
        select
            case
                when exists (
                    select 1
                    from target_base as tgt
                    where tgt.{{ strategy.temporal_cols.get('von') }} > {{ strategy.on_change_value }}
                )
                then fail('Es gibt bereits Daten mit einem groesseren VON')

                when exists (
                    select 1
                    from target_base as tgt
                    where tgt.{{ strategy.temporal_cols.get('bis') }} > {{ strategy.on_change_value }}
                      and tgt.{{ strategy.temporal_cols.get('bis') }} < {{ strategy.temp_max }}
                )
                then fail('Es gibt bereits Daten mit einem groesseren BIS')

                else true
            end as OK__
    ),

    {# Filtert auf 9999-12-30 und berechnet dann die Hashes und liefert daraus die Assertion #}
    snapshotted_data as (
        select
            {%- for col in strategy.key_cols %}
                target_base.{{ col }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            , {%- for col in strategy.business_cols %}
                target_base.{{ col }}{% if not loop.last %},{% endif %}
            {%- endfor %}
            , {{ strategy.unique_key }} as PAYLOAD_KEY__
            , {{ payload_hash_target }} as PAYLOAD_HASH__
            ,{{ strategy.temporal_cols.get('von') }}
            ,{{ strategy.temporal_cols.get('bis') }}
            , aend_zeit
        from target_base
        cross join assert_target a
        where a.OK__
          and target_base.{{ strategy.temporal_cols.get('bis') }} = {{ strategy.temp_max }}
        {% if load_filter_target_condition is not none %}
            and {{ load_filter_target_condition }}
        {% endif %}
    ),
    
    change_actions as (
            select ACTION__
            from ( values ('insert'), ('update') ) t (ACTION__)
    ),
    {# Zweig 1: Key nur in der Quelle -> neuer Satz #}
    new_keys as (
        select
            'insert' as dbt_change_type,
            source_data.*
        from insertions_source_data as source_data
        where not exists (
            select 1
            from snapshotted_data tgt
            where tgt.PAYLOAD_KEY__ = source_data.PAYLOAD_KEY__
        )
    ),

    {# Zweig 2: Key in beiden, Payload verschieden.
       Fan-out: eine Quellzeile erzeugt ZWEI Zeilen (insert + update).
       Der Merge macht daraus: alten Satz abschliessen + neuen Satz einfuegen. #}
    changed_rows as (
        select
            source_data.*
        from insertions_source_data as source_data
        join snapshotted_data tgt
            on tgt.PAYLOAD_KEY__ = source_data.PAYLOAD_KEY__
        where tgt.PAYLOAD_HASH__ <> source_data.PAYLOAD_HASH__
    ),

    

    changed as (
        select
            a.ACTION__ as dbt_change_type,
            c.*
        from changed_rows c
        cross join change_actions a
    ),

    {# Zweig 3: Key nur im Ziel -> abschliessen #}
    invalidations as (
        select
            'invalidate' as dbt_change_type,
            snapshotted_data.*
        from snapshotted_data
        where not exists (
            select 1
            from insertions_source_data as source_data
            where source_data.PAYLOAD_KEY__ = snapshotted_data.PAYLOAD_KEY__
        )
    ),

    deletions as (
        select
            'delete' as dbt_change_type,
            hard_delete_candidates.*
        from hard_delete_candidates
    )

    {# Spaltenliste kommt aus strategy.cols (= Zieltabelle). PAYLOAD_HASH__ und PAYLOAD_KEY__ sind nicht im Ergebnis enthalten #}
    select
        {%- for c in strategy.cols -%}
        {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
            , dbt_change_type
    from new_keys
    union all
    select
        {%- for c in strategy.cols -%}
        {{ ' ' ~ c.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
            , dbt_change_type
    from changed
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
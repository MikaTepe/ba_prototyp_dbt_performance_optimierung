{#
    Inline-Staging variant for the unitemporal_hash snapshot.

Toggle:  dbt snapshot --vars '{"INLINE_STAGING": true}'
#}

{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}

    {% if var('INLINE_STAGING', false) %}
        {#
            Pass: build nothing.
        #}
        {% do return(target_relation) %}
    {% endif %}

    {% set temp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}

{% endmacro %}


{#
    INLINE_STAGING: in variant B the "staging relation" IS the target table, and
    trino__post_snapshot would DROP it.

    POST-HOOK 
#}
{% macro post_snapshot(staging_relation) %}

    {% if staging_relation is none
          or '__dbt_tmp' not in (staging_relation.identifier | string) %}
        {{ log("post_snapshot | skipped, not a temp relation: "
               ~ staging_relation | string, info=true) }}
        {% do return('') %}
    {% endif %}

    {{ adapter.dispatch('post_snapshot', 'dbt')(staging_relation) }}

{% endmacro %}
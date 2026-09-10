{% snapshot INSERT_SILVER_TPCDS_CUSTOMER_STORE %}

{{
    config(
        unique_key=['customer_sk', 'store_sk'],
        strategy='unitemporal',
        alias=var('TAB_FKEY', None),
        schema=var('INR_FKEY'),
        enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),
        temporal_cols={
            "von": "von",
            "bis": "bis"
        },
        temporal_value="VAR('BDAT')",
        meta={
            "INSERT_STATEMENT":"INSERT_SILVER_TPCDS_CUSTOMER_STORE",
            "TAB_FKEY":"silver_tpcds_customer_store",
            "RELEASE_FKEY":"26.0"
        }
    )
}}

with customer_store_relation as (

    /*
        This CTE derives the customer-store relationship from store sales.

        Grain:
            one row per customer_sk + store_sk

        relationship_first_seen_date:
            first date where this customer appeared with this store
            in the sales data.
    */

    select
        ss.ss_customer_sk as customer_sk,
        ss.ss_store_sk as store_sk,
        min(d.d_date) as relationship_first_seen_date

    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    where ss.ss_customer_sk is not null
      and ss.ss_store_sk is not null
      and ss.ss_sold_date_sk is not null

      /*
          The relationship should only include sales that are visible
          up to the current booking date.
      */
      and d.d_date <= DATE '{{ var("BDAT") }}'

    group by
        ss.ss_customer_sk,
        ss.ss_store_sk

),

active_customer_ranked as (

    /*
        Select customer versions that are valid for the current BDAT.

        The row_number is only a safety mechanism in case the simulated
        customer table accidentally contains overlapping active versions.
    */

    select
        c.*,
        row_number() over (
            partition by c.c_customer_sk
            order by c. idh_gltg_fach_adtm desc, c. idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_customer') }} c

    where c. idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
      and c. idh_gltg_fach_edtm > DATE '{{ var("BDAT") }}'

),

active_customer as (

    select *
    from active_customer_ranked
    where rn = 1

),

active_store_ranked as (

    /*
        Select store versions that are valid for the current BDAT.

        This allows simulated store changes, for example a changed
        division name, to propagate into the silver relationship table.
    */

    select
        s.*,
        row_number() over (
            partition by s.s_store_sk
            order by s. idh_gltg_fach_adtm desc, s. idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_store') }} s

    where s. idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
      and s. idh_gltg_fach_edtm > DATE '{{ var("BDAT") }}'

),

active_store as (

    select *
    from active_store_ranked
    where rn = 1

),

relationship_enriched as (

    select
        r.customer_sk,
        r.store_sk,

        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,
        c.c_preferred_cust_flag,
        c.c_birth_country,

        s.s_store_id,
        s.s_store_name,
        s.s_city,
        s.s_state,
        s.s_country,
        s.s_division_id,
        s.s_division_name,
        s.s_company_id,
        s.s_company_name,

        r.relationship_first_seen_date,

        /*
            The relationship version is only valid while:
              - the relationship has already appeared,
              - the customer version is valid,
              - the store version is valid.
        */
        greatest(
            r.relationship_first_seen_date,
            c. idh_gltg_fach_adtm,
            s. idh_gltg_fach_adtm
        ) as  idh_gltg_fach_adtm,

        least(
            c. idh_gltg_fach_edtm,
            s. idh_gltg_fach_edtm
        ) as  idh_gltg_fach_edtm

    from customer_store_relation r

    inner join active_customer c
        on r.customer_sk = c.c_customer_sk

    inner join active_store s
        on r.store_sk = s.s_store_sk

),

final as (

    select
        'bla' as fusi_quel_inst_schl,
        customer_sk,
        store_sk,

        c_customer_id,
        c_first_name,
        c_last_name,
        c_preferred_cust_flag,
        c_birth_country,

        s_store_id,
        s_store_name,
        s_city,
        s_state,
        s_country,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,

        relationship_first_seen_date

    from relationship_enriched

)

select *
from final

{% endsnapshot %}
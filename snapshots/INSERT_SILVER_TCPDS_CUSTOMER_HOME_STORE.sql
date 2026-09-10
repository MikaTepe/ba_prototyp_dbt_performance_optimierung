{% snapshot INSERT_SILVER_TPCDS_CUSTOMER_HOME_STORE %}

{{
    config(
        unique_key=[
            'customer_sk'
        ],
        strategy='bitemporal',
        alias=var('TAB_FKEY', None),
        schema=var('INR_FKEY'),
        enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),
        temporal_cols={
            "von": "tech_ats",
            "bis": "tech_ets"
        },
        temporal_value='VAR("BDAT")',
        meta={
            "INSERT_STATEMENT": "INSERT_SILVER_TPCDS_CUSTOMER_HOME_STORE",
            "TAB_FKEY": "silver_tpcds_customer_home_store",
            "RELEASE_FKEY": "26.0"
        }
    )
}}

with sales_base as (

    select
        ss.ss_customer_sk as customer_sk,
        ss.ss_store_sk as store_sk,
        ss.ss_item_sk as item_sk,
        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_net_paid,
        ss.ss_net_profit,
        d.d_date as purchase_date

    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    where ss.ss_customer_sk is not null
      and ss.ss_store_sk is not null
      and ss.ss_item_sk is not null
      and ss.ss_ticket_number is not null
      and ss.ss_sold_date_sk is not null
      and d.d_date <= DATE '{{ var("BDAT") }}'

),

active_customer_ranked as (

    select
        c.*,
        row_number() over (
            partition by c.c_customer_sk
            order by c.idh_gltg_fach_adtm desc, c.idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_customer') }} c

    where c.idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
      and c.idh_gltg_fach_edtm > DATE '{{ var("BDAT") }}'

),

active_customer as (

    select *
    from active_customer_ranked
    where rn = 1

),

active_store_ranked as (

    select
        s.*,
        row_number() over (
            partition by s.s_store_sk
            order by s.idh_gltg_fach_adtm desc, s.idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_store') }} s

    where s.idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
      and s.idh_gltg_fach_edtm > DATE '{{ var("BDAT") }}'

),

active_store as (

    select *
    from active_store_ranked
    where rn = 1

),

active_item_ranked as (

    select
        i.*,
        row_number() over (
            partition by i.i_item_sk
            order by i.idh_gltg_fach_adtm desc, i.idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_item') }} i

    where i.idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
      and i.idh_gltg_fach_edtm > DATE '{{ var("BDAT") }}'

),

active_item as (

    select *
    from active_item_ranked
    where rn = 1

),

ticket_level_sales as (



    select
        customer_sk,
        store_sk,
        ss_ticket_number,
        purchase_date,

        sum(ss_quantity) as ticket_quantity,
        sum(ss_net_paid) as ticket_net_paid,
        sum(ss_net_profit) as ticket_net_profit

    from sales_base

    group by
        customer_sk,
        store_sk,
        ss_ticket_number,
        purchase_date

),

customer_totals as (

    select
        customer_sk,

        min(purchase_date) as first_purchase_date,
        max(purchase_date) as last_purchase_date,

        count(distinct ss_ticket_number) as customer_total_ticket_count,
        sum(ticket_net_paid) as customer_total_net_paid

    from ticket_level_sales

    group by
        customer_sk

),

customer_store_totals as (

    select
        customer_sk,
        store_sk,

        min(purchase_date) as first_home_store_purchase_date,
        max(purchase_date) as last_home_store_purchase_date,

        count(distinct ss_ticket_number) as home_store_ticket_count,
        sum(ticket_quantity) as home_store_total_quantity,
        sum(ticket_net_paid) as home_store_total_net_paid,
        sum(ticket_net_profit) as home_store_total_net_profit,

        cast(sum(ticket_net_paid) as decimal(38, 6))
            / nullif(cast(count(distinct ss_ticket_number) as decimal(38, 6)), 0)
            as home_store_avg_ticket_value

    from ticket_level_sales

    group by
        customer_sk,
        store_sk

),

customer_store_line_counts as (

    select
        customer_sk,
        store_sk,
        count(*) as home_store_sales_row_count

    from sales_base

    group by
        customer_sk,
        store_sk

),

category_activity as (

    select
        sb.customer_sk,
        sb.store_sk,
        i.i_category_id,
        max(i.i_category) as i_category,

        count(*) as category_sales_row_count,
        sum(sb.ss_net_paid) as category_net_paid

    from sales_base sb

    inner join active_item i
        on sb.item_sk = i.i_item_sk

    where i.i_category_id is not null

    group by
        sb.customer_sk,
        sb.store_sk,
        i.i_category_id

),

category_ranked as (

    select
        *,
        row_number() over (
            partition by customer_sk, store_sk
            order by category_net_paid desc, category_sales_row_count desc, i_category_id
        ) as category_rank

    from category_activity

),

dominant_category as (

    select
        customer_sk,
        store_sk,
        i_category_id as dominant_home_store_category_id,
        i_category as dominant_home_store_category

    from category_ranked

    where category_rank = 1

),

category_counts as (

    select
        customer_sk,
        store_sk,
        count(distinct i_category_id) as distinct_categories_at_home_store

    from category_activity

    group by
        customer_sk,
        store_sk

),

home_store_candidates as (

    select
        cst.customer_sk,
        cst.store_sk,

        ct.first_purchase_date,
        ct.last_purchase_date,
        cst.first_home_store_purchase_date,
        cst.last_home_store_purchase_date,

        cst.home_store_ticket_count,
        coalesce(lc.home_store_sales_row_count, 0) as home_store_sales_row_count,
        cst.home_store_total_quantity,
        cst.home_store_total_net_paid,
        cst.home_store_total_net_profit,
        cst.home_store_avg_ticket_value,

        ct.customer_total_net_paid,
        ct.customer_total_ticket_count,

        cast(cst.home_store_total_net_paid as decimal(38, 6))
            / nullif(cast(ct.customer_total_net_paid as decimal(38, 6)), 0)
            as home_store_paid_share,

        date_diff(
            'day',
            cst.last_home_store_purchase_date,
            DATE '{{ var("BDAT") }}'
        ) as days_since_last_home_store_purchase,

        coalesce(cc.distinct_categories_at_home_store, 0) as distinct_categories_at_home_store,
        dc.dominant_home_store_category_id,
        dc.dominant_home_store_category,


        (
            coalesce(
                cast(cst.home_store_total_net_paid as decimal(38, 6))
                / nullif(cast(ct.customer_total_net_paid as decimal(38, 6)), 0),
                cast(0 as decimal(38, 6))
            ) * cast(100 as decimal(38, 6))
            +
            cast(cst.home_store_ticket_count as decimal(38, 6))
            -
            cast(
                date_diff(
                    'day',
                    cst.last_home_store_purchase_date,
                    DATE '{{ var("BDAT") }}'
                ) as decimal(38, 6)
            ) * cast(0.01 as decimal(38, 6))
        ) as home_store_rank_score

    from customer_store_totals cst

    inner join customer_totals ct
        on cst.customer_sk = ct.customer_sk

    left join customer_store_line_counts lc
        on cst.customer_sk = lc.customer_sk
       and cst.store_sk = lc.store_sk

    left join category_counts cc
        on cst.customer_sk = cc.customer_sk
       and cst.store_sk = cc.store_sk

    left join dominant_category dc
        on cst.customer_sk = dc.customer_sk
       and cst.store_sk = dc.store_sk

),

home_store_ranked as (

    select
        *,
        row_number() over (
            partition by customer_sk
            order by
                home_store_rank_score desc,
                home_store_total_net_paid desc,
                home_store_ticket_count desc,
                last_home_store_purchase_date desc,
                store_sk
        ) as home_store_rank

    from home_store_candidates

),

selected_home_store as (

    select *
    from home_store_ranked
    where home_store_rank = 1

),

relationship_enriched as (

    select
        hs.customer_sk,
        hs.store_sk as home_store_sk,

        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,
        c.c_preferred_cust_flag,
        c.c_birth_country,
        c.c_email_address,
        c.c_birth_year,

        s.s_store_id,
        s.s_store_name,
        s.s_city,
        s.s_state,
        s.s_country,
        s.s_division_id,
        s.s_division_name,
        s.s_company_id,
        s.s_company_name,

        hs.first_purchase_date,
        hs.last_purchase_date,
        hs.first_home_store_purchase_date,
        hs.last_home_store_purchase_date,

        hs.home_store_ticket_count,
        hs.home_store_sales_row_count,
        hs.home_store_total_quantity,
        hs.home_store_total_net_paid,
        hs.home_store_total_net_profit,
        hs.home_store_avg_ticket_value,

        hs.customer_total_net_paid,
        hs.customer_total_ticket_count,
        hs.home_store_paid_share,

        hs.home_store_rank_score,
        cast(hs.days_since_last_home_store_purchase as integer)
            as days_since_last_home_store_purchase,
        hs.distinct_categories_at_home_store,
        hs.dominant_home_store_category_id,
        hs.dominant_home_store_category,


        DATE '{{ var("BDAT") }}' as idh_gltg_fach_adtm,
        DATE '9999-12-31' as idh_gltg_fach_edtm


    from selected_home_store hs

    inner join active_customer c
        on hs.customer_sk = c.c_customer_sk

    inner join active_store s
        on hs.store_sk = s.s_store_sk

),

final as (

    select
        'bla' as fusi_quel_inst_schl,
        customer_sk,
        home_store_sk,

        c_customer_id,
        c_first_name,
        c_last_name,
        c_preferred_cust_flag,
        c_birth_country,
        c_email_address,
        c_birth_year,

        s_store_id,
        s_store_name,
        s_city,
        s_state,
        s_country,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,

        first_purchase_date,
        last_purchase_date,
        first_home_store_purchase_date,
        last_home_store_purchase_date,

        home_store_ticket_count,
        home_store_sales_row_count,
        home_store_total_quantity,
        home_store_total_net_paid,
        home_store_total_net_profit,
        home_store_avg_ticket_value,

        customer_total_net_paid,
        customer_total_ticket_count,
        home_store_paid_share,

        home_store_rank_score,
        days_since_last_home_store_purchase,
        distinct_categories_at_home_store,
        dominant_home_store_category_id,
        dominant_home_store_category,

        idh_gltg_fach_adtm as von,
        idh_gltg_fach_edtm as bis

    from relationship_enriched

)

select *
from final

{% endsnapshot %}
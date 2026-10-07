{{
    config(
        materialized='incremental',
        incremental_strategy='silver_stichtag',

        alias=var('TAB_FKEY', None),
        schema=var('INR_FKEY'),

        enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),

        temporal_column='stichtag',
        temporal_value='VAR("BDAT")',

        meta={
            "INSERT_STATEMENT": "INSERT_SILVER_TPCDS_STORE_CATEGORY_SALES_STICHTAG",
            "TAB_FKEY": "silver_tpcds_store_category_sales_stichtag",
            "RELEASE_FKEY": "26.0"
        }
    )
}}

with runtime_parameters as (

    select
        DATE '{{ var("BDAT", "2025-01-01") }}' as bdat,
        cast(current_timestamp as timestamp(6)) as trans_start

),

sales_base as (

    select
        ss.ss_customer_sk as customer_sk,
        ss.ss_store_sk as store_sk,
        ss.ss_item_sk as item_sk,
        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_net_paid,
        ss.ss_net_profit,
        ss.ss_ext_discount_amt,
        d.d_date as sale_date,
        year(d.d_date) as sales_year,
        month(d.d_date) as sales_month

    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    cross join runtime_parameters p

    where ss.ss_customer_sk is not null
      and ss.ss_store_sk is not null
      and ss.ss_item_sk is not null
      and ss.ss_sold_date_sk is not null
      and d.d_date <= p.bdat

),

active_store_ranked as (

    select
        s.*,
        row_number() over (
            partition by s.s_store_sk
            order by s.idh_gltg_fach_adtm desc, s.idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_store') }} s

    cross join runtime_parameters p

    where s.idh_gltg_fach_adtm <= p.bdat
      and s.idh_gltg_fach_edtm > p.bdat

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

    cross join runtime_parameters p

    where i.idh_gltg_fach_adtm <= p.bdat
      and i.idh_gltg_fach_edtm > p.bdat

),

active_item as (

    select *
    from active_item_ranked
    where rn = 1

),

active_customer_ranked as (

    select
        c.*,
        row_number() over (
            partition by c.c_customer_sk
            order by c.idh_gltg_fach_adtm desc, c.idh_gltg_fach_edtm desc
        ) as rn

    from {{ source('local_lakehouse', 'tpcds_customer') }} c

    cross join runtime_parameters p

    where c.idh_gltg_fach_adtm <= p.bdat
      and c.idh_gltg_fach_edtm > p.bdat

),

active_customer as (

    select *
    from active_customer_ranked
    where rn = 1

),

sales_enriched as (

    select
        sb.sales_year,
        sb.sales_month,

        sb.store_sk,
        i.i_category_id,

        s.s_store_id,
        s.s_store_name,
        s.s_city,
        s.s_state,
        s.s_country,
        s.s_division_id,
        s.s_division_name,
        s.s_company_id,
        s.s_company_name,

        i.i_category,

        sb.customer_sk,
        sb.ss_ticket_number,
        sb.sale_date,
        sb.ss_quantity,
        sb.ss_net_paid,
        sb.ss_net_profit,
        sb.ss_ext_discount_amt,

        c.c_preferred_cust_flag,

        case
            when c.c_customer_sk is not null then 1
            else 0
        end as active_customer_flag

    from sales_base sb

    inner join active_store s
        on sb.store_sk = s.s_store_sk

    inner join active_item i
        on sb.item_sk = i.i_item_sk

    left join active_customer c
        on sb.customer_sk = c.c_customer_sk

    where i.i_category_id is not null

),

aggregated as (

    select
        sales_year,
        sales_month,

        store_sk,
        i_category_id,

        max(s_store_id) as s_store_id,
        max(s_store_name) as s_store_name,
        max(s_city) as s_city,
        max(s_state) as s_state,
        max(s_country) as s_country,
        max(s_division_id) as s_division_id,
        max(s_division_name) as s_division_name,
        max(s_company_id) as s_company_id,
        max(s_company_name) as s_company_name,

        max(i_category) as i_category,

        min(sale_date) as first_sale_date,
        max(sale_date) as last_sale_date,

        count(*) as sales_row_count,
        approx_distinct(customer_sk) as approx_customer_count,
        approx_distinct(ss_ticket_number) as approx_ticket_count,

        sum(cast(coalesce(ss_quantity, 0) as bigint)) as total_quantity,

        cast(sum(coalesce(ss_net_paid, cast(0 as decimal(38, 2)))) as decimal(38, 2))
            as total_net_paid,

        cast(sum(coalesce(ss_net_profit, cast(0 as decimal(38, 2)))) as decimal(38, 2))
            as total_net_profit,

        cast(sum(coalesce(ss_ext_discount_amt, cast(0 as decimal(38, 2)))) as decimal(38, 2))
            as total_discount_amount,

        count_if(c_preferred_cust_flag = 'Y') as preferred_customer_sales_rows,
        sum(active_customer_flag) as active_customer_sales_rows,

        cast(
            sum(coalesce(ss_net_paid, cast(0 as decimal(38, 2))))
            / nullif(cast(count(*) as decimal(38, 6)), cast(0 as decimal(38, 6)))
            as decimal(38, 6)
        ) as avg_net_paid_per_sales_row,

        cast(
            sum(coalesce(ss_net_profit, cast(0 as decimal(38, 2))))
            / nullif(cast(count(*) as decimal(38, 6)), cast(0 as decimal(38, 6)))
            as decimal(38, 6)
        ) as avg_net_profit_per_sales_row

    from sales_enriched

    group by
        sales_year,
        sales_month,
        store_sk,
        i_category_id

),

final as (

    select
        sales_year,
        sales_month,

        store_sk,
        i_category_id,

        s_store_id,
        s_store_name,
        s_city,
        s_state,
        s_country,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,

        i_category,

        first_sale_date,
        last_sale_date,

        sales_row_count,
        approx_customer_count,
        approx_ticket_count,

        total_quantity,
        total_net_paid,
        total_net_profit,
        total_discount_amount,

        preferred_customer_sales_rows,
        active_customer_sales_rows,

        avg_net_paid_per_sales_row,
        avg_net_profit_per_sales_row

    from aggregated

)

select *
from final

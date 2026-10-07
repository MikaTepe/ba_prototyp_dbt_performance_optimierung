{{
    config(
        materialized='incremental',
        incremental_strategy='silver_statisch',

        alias=var('TAB_FKEY', None),
        schema=var('INR_FKEY'),

        enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),

        meta={
            "INSERT_STATEMENT": "INSERT_SILVER_TPCDS_STORE_STATIC",
            "TAB_FKEY": "silver_tpcds_store_static",
            "RELEASE_FKEY": "26.0"
        }
    )
}}


with store_base as (

    select
        s_store_sk,
        s_store_id,
        s_store_name,
        s_number_employees,
        s_floor_space,
        s_hours,
        s_manager,
        s_market_id,
        s_geography_class,
        s_market_desc,
        s_market_manager,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,
        s_street_number,
        s_street_name,
        s_street_type,
        s_suite_number,
        s_city,
        s_county,
        s_state,
        s_zip,
        s_country,
        s_gmt_offset,
        s_tax_percentage

    from {{ source('local_lakehouse', 'tpcds_store') }}

),

store_sales_agg as (

    select
        ss_store_sk,

        min(d.d_date) as first_sale_date,
        max(d.d_date) as last_sale_date,

        count(*) as sales_row_count,
        count(distinct ss_ticket_number) as ticket_count,
        count(distinct ss_customer_sk) as distinct_customer_count,

        sum(coalesce(ss_quantity, 0)) as total_quantity,
        sum(coalesce(ss_net_paid, 0)) as total_net_paid,
        sum(coalesce(ss_net_profit, 0)) as total_net_profit,

        case
            when count(distinct ss_ticket_number) = 0 then cast(null as decimal(18, 2))
            else cast(sum(coalesce(ss_net_paid, 0)) / count(distinct ss_ticket_number) as decimal(18, 2))
        end as avg_ticket_net_paid

    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    left join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    where d.d_date <= cast('{{ var("BDAT") }}' as date)

    group by
        ss_store_sk

),

final as (

    select
        'bla' as fusi_quel_inst_schl,

        sb.s_store_sk,
        sb.s_store_id,
        sb.s_store_name,
        sb.s_number_employees,
        sb.s_floor_space,
        sb.s_hours,
        sb.s_manager,
        sb.s_market_id,
        sb.s_geography_class,
        sb.s_market_desc,
        sb.s_market_manager,
        sb.s_division_id,
        sb.s_division_name,
        sb.s_company_id,
        sb.s_company_name,
        sb.s_street_number,
        sb.s_street_name,
        sb.s_street_type,
        sb.s_suite_number,
        sb.s_city,
        sb.s_county,
        sb.s_state,
        sb.s_zip,
        sb.s_country,
        sb.s_gmt_offset,
        sb.s_tax_percentage,

        ssa.first_sale_date,
        ssa.last_sale_date,
        coalesce(ssa.sales_row_count, 0) as sales_row_count,
        coalesce(ssa.ticket_count, 0) as ticket_count,
        coalesce(ssa.distinct_customer_count, 0) as distinct_customer_count,
        coalesce(ssa.total_quantity, 0) as total_quantity,
        coalesce(ssa.total_net_paid, 0) as total_net_paid,
        coalesce(ssa.total_net_profit, 0) as total_net_profit,
        ssa.avg_ticket_net_paid

    from store_base sb

    left join store_sales_agg ssa
        on sb.s_store_sk = ssa.ss_store_sk

)

select *
from final
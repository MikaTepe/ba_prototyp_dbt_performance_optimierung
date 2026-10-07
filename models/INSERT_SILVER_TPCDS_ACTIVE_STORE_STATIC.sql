{{
    config(
        materialized='incremental',
        incremental_strategy='silver_statisch',

        alias=var('TAB_FKEY', None),
        schema=var('INR_FKEY'),

        enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),

        meta={
            "INSERT_STATEMENT": "INSERT_SILVER_TPCDS_ACTIVE_STORE_STATIC",
            "TAB_FKEY": "silver_tpcds_active_store_static",
            "RELEASE_FKEY": "26.0"
        }
    )
}}


{%- set bdat = var('BDAT', none) -%}

{%- if bdat is none -%}
  {%- set as_of_date_sql = "current_date" -%}
{%- else -%}
  {%- set as_of_date_sql = "cast('" ~ bdat ~ "' as date)" -%}
{%- endif -%}

with sales_until_bdat as (

    select
        ss.ss_store_sk,

        min(d.d_date) as first_sale_date,
        max(d.d_date) as last_sale_date_as_of,

        count(*) as sales_row_count_as_of,
        count(distinct ss.ss_ticket_number) as ticket_count_as_of,
        count(distinct ss.ss_customer_sk) as distinct_customer_count_as_of,

        sum(coalesce(ss.ss_quantity, 0)) as total_quantity_as_of,
        sum(coalesce(ss.ss_net_paid, 0)) as total_net_paid_as_of,
        sum(coalesce(ss.ss_net_profit, 0)) as total_net_profit_as_of

    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    where d.d_date <= {{ as_of_date_sql }}

    group by
        ss.ss_store_sk

),

active_stores as (

    select
        'bla' as fusi_quel_inst_schl,

        {{ as_of_date_sql }} as as_of_date,

        s.s_store_sk,
        s.s_store_id,
        s.s_store_name,
        s.s_number_employees,
        s.s_floor_space,
        s.s_hours,
        s.s_manager,
        s.s_market_id,
        s.s_geography_class,
        s.s_market_desc,
        s.s_market_manager,
        s.s_division_id,
        s.s_division_name,
        s.s_company_id,
        s.s_company_name,
        s.s_street_number,
        s.s_street_name,
        s.s_street_type,
        s.s_suite_number,
        s.s_city,
        s.s_county,
        s.s_state,
        s.s_zip,
        s.s_country,
        s.s_gmt_offset,
        s.s_tax_percentage,

        su.first_sale_date,
        su.last_sale_date_as_of,
        su.sales_row_count_as_of,
        su.ticket_count_as_of,
        su.distinct_customer_count_as_of,
        su.total_quantity_as_of,
        su.total_net_paid_as_of,
        su.total_net_profit_as_of,

        case
            when su.sales_row_count_as_of > 0 then true
            else false
        end as is_active_as_of_date

    from {{ source('local_lakehouse', 'tpcds_store') }} s

    inner join sales_until_bdat su
        on s.s_store_sk = su.ss_store_sk

)

select *
from active_stores
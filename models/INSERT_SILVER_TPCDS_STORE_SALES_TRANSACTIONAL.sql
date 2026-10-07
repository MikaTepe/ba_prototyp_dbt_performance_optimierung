{{
  config(
    materialized='incremental',
    incremental_strategy='silver_transaktional',

    alias=var('TAB_FKEY', None),
    schema=var('INR_FKEY'),

    enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),

    temporal_column='TRANS_TS',
    temporal_value='var("BDAT")',

    unique_key=['ticket_number', 'item_sk', 'customer_sk', 'store_sk']
  )
}}

with sales_for_booking_day as (

    select
        ss.*
    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    where d.d_date = cast('{{ var("BDAT") }}' as date)

),

enriched as (

    select
        cast('{{ var("INR_FKEY") }}' as varchar) as fusi_quel_inst_schl,

        ss.ss_ticket_number as ticket_number,
        ss.ss_item_sk as item_sk,
        i.i_item_id as item_id,
        i.i_product_name as item_name,
        i.i_category as item_category,
        i.i_class as item_class,
        i.i_brand as item_brand,

        ss.ss_customer_sk as customer_sk,
        c.c_customer_id as customer_id,
        c.c_first_name as customer_first_name,
        c.c_last_name as customer_last_name,

        ss.ss_store_sk as store_sk,
        s.s_store_id as store_id,
        s.s_store_name as store_name,
        s.s_city as store_city,
        s.s_state as store_state,
        s.s_country as store_country,
        d.d_date as buchungs_dtm,
        case
            when ss.ss_ticket_number % 5 = 0
                then date_add('day', 1, d.d_date)
            else d.d_date
        end as valuta_dtm,

        ss.ss_quantity as quantity,
        ss.ss_wholesale_cost as wholesale_cost,
        ss.ss_list_price as list_price,
        ss.ss_sales_price as sales_price,
        ss.ss_ext_discount_amt as ext_discount_amt,
        ss.ss_ext_sales_price as ext_sales_price,
        ss.ss_ext_wholesale_cost as ext_wholesale_cost,
        ss.ss_ext_list_price as ext_list_price,
        ss.ss_ext_tax as ext_tax,
        ss.ss_coupon_amt as coupon_amt,
        ss.ss_net_paid as net_paid,
        ss.ss_net_paid_inc_tax as net_paid_inc_tax,
        ss.ss_net_profit as net_profit,

        current_timestamp as aend_zeit

    from sales_for_booking_day ss

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk

    left join {{ source('local_lakehouse', 'tpcds_item') }} i
        on ss.ss_item_sk = i.i_item_sk

    left join {{ source('local_lakehouse', 'tpcds_customer') }} c
        on ss.ss_customer_sk = c.c_customer_sk

    left join {{ source('local_lakehouse', 'tpcds_store') }} s
        on ss.ss_store_sk = s.s_store_sk

)

select *
from enriched
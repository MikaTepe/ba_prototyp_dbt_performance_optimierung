{% snapshot INSERT_SILVER_TPCDS_STORE_SALES_AGG %}

{{
    config(
        unique_key=['ss_customer_sk','ss_store_sk','i_category_id','d_year','d_month'],
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
            "INSERT_STATEMENT": "INSERT_SILVER_TPCDS_STORE_SALES_AGG",
            "TAB_FKEY": "silver_tpcds_store_sales_agg",
            "RELEASE_FKEY": "26.0"
        }
    )
}}

with store_sales_filtered as (

    select
        ss.ss_customer_sk,
        ss.ss_item_sk,
        ss.ss_store_sk,
        ss.ss_sold_date_sk,
        ss.ss_ticket_number,

        ss.ss_quantity,
        ss.ss_sales_price,
        ss.ss_net_paid,
        ss.ss_net_profit

    from {{ source('local_lakehouse', 'tpcds_store_sales') }} ss

    where ss.ss_customer_sk is not null
      and ss.ss_item_sk is not null
      and ss.ss_store_sk is not null
      and ss.ss_sold_date_sk is not null

),

sales_enriched as (

    select
        ss.ss_customer_sk,
        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,

        ss.ss_store_sk,
        s.s_store_id,
        s.s_store_name,
        s.s_state,
        s.s_country,

        ss.ss_item_sk,
        i.i_item_id,
        i.i_category_id,
        i.i_category,
        i.i_class,
        i.i_brand,

        d.d_date,
        d.d_year,
        d.d_moy as d_month,
        d.d_qoy as d_quarter,

        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_sales_price,
        ss.ss_net_paid,
        ss.ss_net_profit,

        c.idh_gltg_fach_adtm as customer_idh_gltg_fach_adtm,
        c.idh_gltg_fach_edtm as customer_idh_gltg_fach_edtm,
        i.idh_gltg_fach_adtm as item_idh_gltg_fach_adtm,
        i.idh_gltg_fach_edtm as item_idh_gltg_fach_edtm,
        s.idh_gltg_fach_adtm as store_idh_gltg_fach_adtm,
        s.idh_gltg_fach_edtm as store_idh_gltg_fach_edtm

    from store_sales_filtered ss

    inner join {{ source('local_lakehouse', 'tpcds_customer') }} c
        on ss.ss_customer_sk = c.c_customer_sk
       and c.idh_gltg_fach_adtm <= DATE '{{ var("BDAT" ) }}'
       and c.idh_gltg_fach_edtm > DATE '{{ var("BDAT" ) }}'

    inner join {{ source('local_lakehouse', 'tpcds_item') }} i
        on ss.ss_item_sk = i.i_item_sk
       and i.idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
       and i.idh_gltg_fach_edtm > DATE '{{ var("BDAT" ) }}'

    inner join {{ source('local_lakehouse', 'tpcds_store') }} s
        on ss.ss_store_sk = s.s_store_sk
       and s.idh_gltg_fach_adtm <= DATE '{{ var("BDAT") }}'
       and s.idh_gltg_fach_edtm > DATE '{{ var("BDAT" ) }}'

    inner join {{ source('local_lakehouse', 'tpcds_date_dim') }} d
        on ss.ss_sold_date_sk = d.d_date_sk
       and d.d_date <= DATE '{{ var("BDAT" ) }}'

),

monthly_sales_agg as (

    select
        ss_customer_sk,
        ss_store_sk,
        i_category_id,
        d_year,
        d_month,

        max(c_customer_id) as c_customer_id,
        max(c_first_name) as c_first_name,
        max(c_last_name) as c_last_name,

        max(s_store_id) as s_store_id,
        max(s_store_name) as s_store_name,
        max(s_state) as s_state,
        max(s_country) as s_country,

        max(i_category) as i_category,
        max(d_quarter) as d_quarter,

        count(*) as row_count,
        count(distinct ss_ticket_number) as ticket_count,

        count(distinct i_class) as distinct_item_classes,
        count(distinct i_brand) as distinct_item_brands,

        sum(ss_quantity) as total_quantity,
        sum(ss_net_paid) as total_net_paid,
        sum(ss_net_profit) as total_net_profit,

        avg(ss_sales_price) as avg_sales_price,
        min(ss_sales_price) as min_sales_price,
        max(ss_sales_price) as max_sales_price,

        min(d_date) as first_sales_date,
        max(d_date) as last_sales_date,


        max(
            least(
                customer_idh_gltg_fach_adtm,
                item_idh_gltg_fach_adtm,
                store_idh_gltg_fach_adtm
            )
        ) as idh_gltg_fach_adtm,

        min(
            greatest(
                customer_idh_gltg_fach_edtm,
                item_idh_gltg_fach_edtm,
                store_idh_gltg_fach_edtm
            )
        ) as idh_gltg_fach_edtm

    from sales_enriched

    where i_category_id is not null

    group by
        ss_customer_sk,
        ss_store_sk,
        i_category_id,
        d_year,
        d_month


),

final as (

    select
        'bla' as fusi_quel_inst_schl,
        ss_customer_sk,
        c_customer_id,
        c_first_name,
        c_last_name,

        ss_store_sk,
        s_store_id,
        s_store_name,
        s_state,
        s_country,

        i_category_id,
        i_category,

        d_year,
        d_month,
        d_quarter,

        row_count,
        ticket_count,
        distinct_item_classes,
        distinct_item_brands,

        total_quantity,
        total_net_paid,
        total_net_profit,
        avg_sales_price,
        min_sales_price,
        max_sales_price,
        first_sales_date,
        last_sales_date

    from monthly_sales_agg

)

select *
from final

{% endsnapshot %}
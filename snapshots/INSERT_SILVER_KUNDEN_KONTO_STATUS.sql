{% snapshot INSERT_SILVER_KUNDEN_KONTO_STATUS %}
{{
    config(
        unique_key=['kunden_nr', 'konto_nr'],
        strategy='unitemporal',
        alias=var('TAB_FKEY', None),
        schema=var('INR_FKEY'),
        enabled=(this.name == var('ACTIVE_SNAPSHOT', this.name)),
        temporal_cols={
            "von": "von",
            "bis": "bis"
        },
        temporal_value="ULTIMO(TO_DATE('2026-05-05'))",
        meta={
            "INSERT_STATEMENT":"INSERT_SILVER_KUNDEN_KONTO_STATUS",
            "TAB_FKEY":"silver_kunden_konto_status",
            "RELEASE_FKEY":"25.1"
        }
    )
}}

with source_data as (
    select *
    from {{ source("local_lakehouse", "bronze_kunden_konto_feed_1") }}
),

cleaned_source as (
    select
        trim(src_system) as src_system,
        trim(raw_file_name) as raw_file_name,
        upper(trim(raw_event_type)) as raw_event_type,

        kunden_nr,
        konto_nr,
        upper(trim(iban)) as iban,

        produkt_code,
        upper(trim(produkt_name)) as produkt_name,
        upper(trim(konto_status)) as konto_status,
        upper(trim(waehrung_code)) as waehrung_code,
        upper(trim(branch_code)) as branch_code,

        upper(trim(kunden_typ)) as kunden_typ,
        upper(trim(kunden_segment)) as kunden_segment,
        upper(trim(aufenthaltsstaat)) as aufenthaltsstaat,

        risiko_klasse,
        upper(trim(kyc_status)) as kyc_status,
        berater_id,

        idh_gltg_fach_adtm,
        idh_gltg_fach_edtm,
        trans_start,

        aktuell_saldo,
        kredit_limit,
        zins_rate,
        ueberziehung_erlaubt

    from source_data

    where konto_nr is not null
      and kunden_nr is not null
      and iban is not null
      and produkt_code is not null
),

deduplicated_source as (
    select *
    from (

        select
        cleaned_source.*,
        row_number() over (
            partition by konto_nr
            order by
                trans_start desc,
                idh_gltg_fach_adtm desc,
                raw_file_name desc,
                kunden_nr desc
        ) as rn
        from cleaned_source
    ) ranked_source

    where rn = 1
),

business_rules as (
    select
        case
            when src_system is not null or src_system = ''
                then 'UNKNOWN'
            else src_system
        end as src_system,

        kunden_nr,
        konto_nr,
        iban,

        case
            when produkt_code is null
                then 'UNKNOWN'
            else 'P' || lpad(cast(produkt_code as varchar), 4, '0')
        end as produkt_code,

        case
            when konto_status in ('AKTIV', 'ACTIVE')
                then 'ACTIVE'
            when konto_status in ('GESPERRT', 'BLOCKED', 'SPERRE')
                then 'BLOCKED'
            when konto_status in ('GEKUENDIGT', 'CLOSED', 'CANCELLED')
                then 'CLOSED'
            when konto_status in ('IN_PRUEFUNG', 'PENDING', 'REVIEW')
                then 'UNDER_REVIEW'
            else 'UNKNOWN'
        end as konto_status,

        case
            when waehrung_code in ('EUR', 'USD', 'CHF', 'GBP')
                then waehrung_code
            else 'UNKNOWN'
        end as waehrung_code,

        case
            when branch_code is null or branch_code = ''
                then 'UNKNOWN'
            else branch_code
        end as branch_code,

        case
            when kunden_segment in ('VIP', 'PRIVATE_BANKING')
                then 'VIP'
            when kunden_segment in ('PREMIUM', 'AFFLUENT')
                then 'PREMIUM'
            when kunden_segment in ('JUNIOR', 'STUDENT')
                then 'JUNIOR'
            when kunden_segment in ('STANDARD', 'BASIC')
                then 'STANDARD'
            else 'UNKNOWN'
        end as kunden_segment,

        case
            when risiko_klasse between 1 and 2
                then 'LOW'
            when risiko_klasse = 3
                then 'MEDIUM'
            when risiko_klasse between 4 and 5
                then 'HIGH'
            else 'UNKNOWN'
        end as risiko_klasse,

        case
            when kyc_status in ('VOLLSTAENDIG', 'COMPLETE', 'APPROVED')
                then 'COMPLETE'
            when kyc_status in ('OFFEN', 'OPEN', 'MISSING')
                then 'OPEN'
            when kyc_status in ('IN_PRUEFUNG', 'REVIEW', 'PENDING')
                then 'IN_REVIEW'
            when kyc_status in ('ABGELEHNT', 'REJECTED')
                then 'REJECTED'
            else 'UNKNOWN'
        end as kyc_status,

        case
            when idh_gltg_fach_adtm is not null
                then idh_gltg_fach_adtm
            when trans_start is not null
                then trans_start
            else current_date
        end as idh_gltg_fach_adtm,

        case
            when konto_status in ('GEKUENDIGT', 'CLOSED', 'CANCELLED')
                 and idh_gltg_fach_edtm is not null
                 and idh_gltg_fach_edtm < date '9999-12-31'
                then idh_gltg_fach_edtm

            when konto_status in ('GEKUENDIGT', 'CLOSED', 'CANCELLED')
                 and idh_gltg_fach_edtm is null
                then trans_start

            else date '9999-12-31'
        end as idh_gltg_fach_edtm

    from deduplicated_source

),

quality_checked as (
    select *
    from business_rules
    where kunden_nr is not null
      and konto_nr is not null
      and iban is not null
      and idh_gltg_fach_adtm <= {{ ultimo("TO_DATE('2026-05-05')", 'date') }}
      and idh_gltg_fach_edtm > {{ ultimo("TO_DATE('2026-05-05')", 'date') }}
)

select
    'bla' as fusi_quel_inst_schl,
    src_system,
    kunden_nr,
    konto_nr,
    iban,
    produkt_code,
    konto_status,
    waehrung_code,
    branch_code,
    kunden_segment,
    risiko_klasse,
    kyc_status

    from quality_checked


{% endsnapshot %}
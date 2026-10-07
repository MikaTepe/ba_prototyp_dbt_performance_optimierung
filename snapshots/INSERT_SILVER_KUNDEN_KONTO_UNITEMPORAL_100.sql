{% snapshot INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_100 %}

{{
    config(
        unique_key=['kunden_nr', 'konto_nr', 'iban', 'geburtsdatum'],
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
            "INSERT_STATEMENT":"INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_100",
            "TAB_FKEY":"silver_kunden_konto_unitemporal_100",
            "RELEASE_FKEY":"25.1"
        }
    )
}}



with source_data as (
    select *
    from {{ source('local_lakehouse', 'bronze_kunden_konto_feed_10mil_test') }}
),

cleaned_source as (
    select
        upper(trim(src_system)) as src_system,
        upper(trim(raw_file_name)) as raw_file_name,
        upper(trim(raw_event_type)) as raw_event_type,
        trim(load_batch_id) as load_batch_id,
        source_row_id,
        trim(kunden_nr) as kunden_nr,
        trim(konto_nr) as konto_nr,
        upper(trim(iban)) as iban,
        trim(konto_typ) as konto_typ,
        trim(konto_art) as konto_art,
        produkt_code,
        upper(trim(produkt_name)) as produkt_name,
        trim(produkt_gruppe) as produkt_gruppe,
        trim(produkt_kategorie) as produkt_kategorie,
        upper(trim(konto_status)) as konto_status,
        upper(trim(waehrung_code)) as waehrung_code,
        upper(trim(branch_code)) as branch_code,
        trim(branch_name) as branch_name,
        trim(branch_region) as branch_region,
        upper(trim(kunden_typ)) as kunden_typ,
        upper(trim(kunden_segment)) as kunden_segment,
        upper(trim(kunden_subsegment)) as kunden_subsegment,
        upper(trim(aufenthaltsstaat)) as aufenthaltsstaat,
        upper(trim(steuer_land)) as steuer_land,
        upper(trim(nationalitaet)) as nationalitaet,
        risiko_klasse,
        upper(trim(kyc_status)) as kyc_status,
        upper(trim(aml_status)) as aml_status,
        pep_flag,
        upper(trim(berater_id)) as berater_id,
        upper(trim(berater_team)) as berater_team,
        upper(trim(vertriebs_kanal)) as vertriebs_kanal,
        idh_gltg_fach_adtm,
        idh_gltg_fach_edtm,
        buchungstag,
        aktuell_saldo,
        kredit_limit,
        zins_rate,
        ueberziehung_erlaubt,
        monatlicher_eingang,
        monatlicher_ausgang,
        saldo_vormonat,
        dispo_limit,
        gebuehren_monat,
        offene_posten_anzahl,
        letzte_transaktion_ts,
        letzte_transaktion_betrag,
        anzahl_transaktionen_30t,
        anzahl_transaktionen_90t,
        kredit_score,
        upper(trim(rating_code)) as rating_code,
        upper(trim(scoring_modell)) as scoring_modell,
        default_wahrscheinlichkeit,
        sicherheiten_wert,
        beleihungsquote,
        fraud_score,
        upper(trim(fraud_status)) as fraud_status,
        consent_marketing,
        consent_onlinebanking,
        digital_aktiv,
        app_nutzung_30t,
        online_login_30t,
        papierlos_flag,
        kontostand_warnung_flag,
        letzter_login_ts,
        iban_valid_flag,
        adresse_valid_flag,
        email_valid_flag,
        telefon_valid_flag,
        geburtsdatum,
        kunden_alter,
        upper(trim(plz)) as plz,
        upper(trim(ort)) as ort,
        upper(trim(bundesland)) as bundesland,
        trim(strasse_hash) as strasse_hash,
        upper(trim(email_domain)) as email_domain,
        upper(trim(telefon_land_code)) as telefon_land_code,
        upper(trim(arbeitgeber_code)) as arbeitgeber_code,
        upper(trim(branche_code)) as branche_code,
        upper(trim(beschaeftigungsstatus)) as beschaeftigungsstatus,
        upper(trim(einkommensklasse)) as einkommensklasse,
        upper(trim(familienstand)) as familienstand,
        anzahl_kinder,
        upper(trim(wohnstatus)) as wohnstatus,
        immobilienbesitz_flag,
        trim(steuer_id_hash) as steuer_id_hash,
        upper(trim(ausweis_typ)) as ausweis_typ,
        ausweis_gueltig_bis,
        vertragsbeginn,
        vertragsende,
        upper(trim(tarif_code)) as tarif_code,
        upper(trim(kundenwert_segment)) as kundenwert_segment,
        cross_sell_score,
        churn_score,
        upper(trim(kampagne_code)) as kampagne_code,
        trim(bemerkung) as bemerkung,
        trim(payload_hash) as payload_hash
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
        case when src_system is null or src_system = '' then 'UNKNOWN' else src_system end as src_system,
        kunden_nr,
        konto_nr,
        iban,
        case when produkt_code is null then 'UNKNOWN' else 'P' || lpad(cast(produkt_code as varchar), 4, '0') end as produkt_code,
        produkt_name,
        case
    when konto_status in ('AKTIV', 'ACTIVE') then 'ACTIVE'
    when konto_status in ('GESPERRT', 'BLOCKED', 'SPERRE') then 'BLOCKED'
    when konto_status in ('GEKUENDIGT', 'CLOSED', 'CANCELLED') then 'CLOSED'
    when konto_status in ('IN_PRUEFUNG', 'PENDING', 'REVIEW') then 'UNDER_REVIEW'
    else 'UNKNOWN'
end as konto_status,
        case when waehrung_code in ('EUR', 'USD', 'CHF', 'GBP') then waehrung_code else 'UNKNOWN' end as waehrung_code,
        case when branch_code is null or branch_code = '' then 'UNKNOWN' else branch_code end as branch_code,
        kunden_typ,
        case
    when kunden_segment in ('VIP', 'PRIVATE_BANKING') then 'VIP'
    when kunden_segment in ('PREMIUM', 'AFFLUENT') then 'PREMIUM'
    when kunden_segment in ('JUNIOR', 'STUDENT') then 'JUNIOR'
    when kunden_segment in ('STANDARD', 'BASIC') then 'STANDARD'
    else 'UNKNOWN'
end as kunden_segment,
        aufenthaltsstaat,
        case
    when risiko_klasse between 1 and 2 then 'LOW'
    when risiko_klasse = 3 then 'MEDIUM'
    when risiko_klasse between 4 and 5 then 'HIGH'
    else 'UNKNOWN'
end as risiko_klasse,
        case
    when kyc_status in ('VOLLSTAENDIG', 'COMPLETE', 'APPROVED') then 'COMPLETE'
    when kyc_status in ('OFFEN', 'OPEN', 'MISSING') then 'OPEN'
    when kyc_status in ('IN_PRUEFUNG', 'REVIEW', 'PENDING') then 'IN_REVIEW'
    when kyc_status in ('ABGELEHNT', 'REJECTED') then 'REJECTED'
    else 'UNKNOWN'
end as kyc_status,
        berater_id,
        aktuell_saldo,
        kredit_limit,
        zins_rate,
        ueberziehung_erlaubt,
        buchungstag,
        payload_hash,
        konto_typ,
        konto_art,
        produkt_gruppe,
        produkt_kategorie,
        branch_name,
        branch_region,
        kunden_subsegment,
        steuer_land,
        nationalitaet,
        aml_status,
        pep_flag,
        berater_team,
        vertriebs_kanal,
        monatlicher_eingang,
        monatlicher_ausgang,
        saldo_vormonat,
        dispo_limit,
        gebuehren_monat,
        offene_posten_anzahl,
        letzte_transaktion_ts,
        letzte_transaktion_betrag,
        anzahl_transaktionen_30t,
        anzahl_transaktionen_90t,
        kredit_score,
        rating_code,
        scoring_modell,
        default_wahrscheinlichkeit,
        sicherheiten_wert,
        beleihungsquote,
        fraud_score,
        fraud_status,
        consent_marketing,
        consent_onlinebanking,
        digital_aktiv,
        app_nutzung_30t,
        online_login_30t,
        papierlos_flag,
        kontostand_warnung_flag,
        letzter_login_ts,
        iban_valid_flag,
        adresse_valid_flag,
        email_valid_flag,
        telefon_valid_flag,
        geburtsdatum,
        kunden_alter,
        plz,
        ort,
        bundesland,
        strasse_hash,
        email_domain,
        telefon_land_code,
        arbeitgeber_code,
        branche_code,
        beschaeftigungsstatus,
        einkommensklasse,
        familienstand,
        anzahl_kinder,
        wohnstatus,
        immobilienbesitz_flag,
        steuer_id_hash,
        ausweis_typ,
        ausweis_gueltig_bis,
        vertragsbeginn,
        vertragsende,
        tarif_code,
        kundenwert_segment,
        cross_sell_score,
        churn_score,
        kampagne_code,
        bemerkung,
        case
    when idh_gltg_fach_adtm is not null then idh_gltg_fach_adtm
    else current_date
end as idh_gltg_fach_adtm,
        case
    when konto_status in ('GEKUENDIGT', 'CLOSED', 'CANCELLED')
         and idh_gltg_fach_edtm is not null
         and idh_gltg_fach_edtm < date '9999-12-31'
        then idh_gltg_fach_edtm
    when konto_status in ('GEKUENDIGT', 'CLOSED', 'CANCELLED')
         and idh_gltg_fach_edtm is null
        then cast(current_timestamp as date)
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
      and idh_gltg_fach_adtm <= date '{{ var("BDAT") }}'
      and idh_gltg_fach_edtm > date '{{ var("BDAT") }}'
)

select
    'bla' as fusi_quel_inst_schl,
    src_system,
    kunden_nr,
    konto_nr,
    iban,
    produkt_code,
    produkt_name,
    konto_status,
    waehrung_code,
    branch_code,
    kunden_typ,
    kunden_segment,
    aufenthaltsstaat,
    risiko_klasse,
    kyc_status,
    berater_id,
    aktuell_saldo,
    kredit_limit,
    zins_rate,
    ueberziehung_erlaubt,
    buchungstag,
    payload_hash,
    konto_typ,
    konto_art,
    produkt_gruppe,
    produkt_kategorie,
    branch_name,
    branch_region,
    kunden_subsegment,
    steuer_land,
    nationalitaet,
    aml_status,
    pep_flag,
    berater_team,
    vertriebs_kanal,
    monatlicher_eingang,
    monatlicher_ausgang,
    saldo_vormonat,
    dispo_limit,
    gebuehren_monat,
    offene_posten_anzahl,
    letzte_transaktion_ts,
    letzte_transaktion_betrag,
    anzahl_transaktionen_30t,
    anzahl_transaktionen_90t,
    kredit_score,
    rating_code,
    scoring_modell,
    default_wahrscheinlichkeit,
    sicherheiten_wert,
    beleihungsquote,
    fraud_score,
    fraud_status,
    consent_marketing,
    consent_onlinebanking,
    digital_aktiv,
    app_nutzung_30t,
    online_login_30t,
    papierlos_flag,
    kontostand_warnung_flag,
    letzter_login_ts,
    iban_valid_flag,
    adresse_valid_flag,
    email_valid_flag,
    telefon_valid_flag,
    geburtsdatum,
    kunden_alter,
    plz,
    ort,
    bundesland,
    strasse_hash,
    email_domain,
    telefon_land_code,
    arbeitgeber_code,
    branche_code,
    beschaeftigungsstatus,
    einkommensklasse,
    familienstand,
    anzahl_kinder,
    wohnstatus,
    immobilienbesitz_flag,
    steuer_id_hash,
    ausweis_typ,
    ausweis_gueltig_bis,
    vertragsbeginn,
    vertragsende,
    tarif_code,
    kundenwert_segment,
    cross_sell_score,
    churn_score,
    kampagne_code,
    bemerkung
from quality_checked

{% endsnapshot %}
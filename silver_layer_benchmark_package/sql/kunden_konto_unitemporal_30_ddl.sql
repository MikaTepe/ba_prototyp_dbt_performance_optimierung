-- Trino / Starburst Iceberg DDL
-- Adjust catalog/schema names if needed.
CREATE TABLE IF NOT EXISTS local_lakehouse.silver.kunden_konto_unitemporal_30 (
    inr_nr varchar,
    fkey varchar,
    skey varchar,
    rkey bigint,
    sdat date,
    edat date,
    is_current boolean,
    load_ts timestamp(6),
    src_system varchar,
    kunden_nr varchar,
    konto_nr varchar,
    iban varchar,
    produkt_code varchar,
    produkt_name varchar,
    konto_status varchar,
    waehrung_code varchar,
    branch_code varchar,
    kunden_typ varchar,
    kunden_segment varchar,
    aufenthaltsstaat varchar,
    risiko_klasse varchar,
    kyc_status varchar,
    berater_id varchar,
    aktuell_saldo decimal(18,2),
    kredit_limit decimal(18,2),
    zins_rate decimal(9,6),
    ueberziehung_erlaubt boolean,
    buchungstag date,
    payload_hash varchar,
    ingestion_ts timestamp(6)
)
WITH (
    format = 'PARQUET'
);

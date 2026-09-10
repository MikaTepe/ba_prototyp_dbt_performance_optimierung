#!/usr/bin/env python3
"""
Synthetic source-data loader for the 100-column silver-layer benchmark table.

Requirements:
    pip install trino

Example:
    python fill_source_bronze_kunden_konto_feed_100.py \
        --host localhost --port 8080 --user trino \
        --catalog local_lakehouse --schema bronze \
        --table bronze_kunden_konto_feed_100 \
        --rows 100000 --batch-size 500 --clear-table

Notes:
- The script uses batched INSERT statements, which is simple and portable but not the fastest loading method.
- For very large datasets, generating Parquet files directly into your object storage may be faster.
"""

from __future__ import annotations

import argparse
import hashlib
import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable
import os
import trino
from trino.auth import OAuth2Authentication

try:
    import trino
except ImportError as exc:
    raise SystemExit("Missing dependency: pip install trino") from exc

COLUMNS = [
    'src_system',
    'raw_file_name',
    'raw_event_type',
    'load_batch_id',
    'source_row_id',
    'ingestion_ts',
    'kunden_nr',
    'konto_nr',
    'iban',
    'konto_typ',
    'konto_art',
    'produkt_code',
    'produkt_name',
    'produkt_gruppe',
    'produkt_kategorie',
    'konto_status',
    'waehrung_code',
    'branch_code',
    'branch_name',
    'branch_region',
    'kunden_typ',
    'kunden_segment',
    'kunden_subsegment',
    'aufenthaltsstaat',
    'steuer_land',
    'nationalitaet',
    'risiko_klasse',
    'kyc_status',
    'aml_status',
    'pep_flag',
    'berater_id',
    'berater_team',
    'vertriebs_kanal',
    'idh_gltg_fach_adtm',
    'idh_gltg_fach_edtm',
    'trans_start',
    'buchungstag',
    'aktuell_saldo',
    'kredit_limit',
    'zins_rate',
    'ueberziehung_erlaubt',
    'monatlicher_eingang',
    'monatlicher_ausgang',
    'saldo_vormonat',
    'dispo_limit',
    'gebuehren_monat',
    'offene_posten_anzahl',
    'letzte_transaktion_ts',
    'letzte_transaktion_betrag',
    'anzahl_transaktionen_30t',
    'anzahl_transaktionen_90t',
    'kredit_score',
    'rating_code',
    'scoring_modell',
    'default_wahrscheinlichkeit',
    'sicherheiten_wert',
    'beleihungsquote',
    'fraud_score',
    'fraud_status',
    'consent_marketing',
    'consent_onlinebanking',
    'digital_aktiv',
    'app_nutzung_30t',
    'online_login_30t',
    'papierlos_flag',
    'kontostand_warnung_flag',
    'letzter_login_ts',
    'iban_valid_flag',
    'adresse_valid_flag',
    'email_valid_flag',
    'telefon_valid_flag',
    'geburtsdatum',
    'kunden_alter',
    'plz',
    'ort',
    'bundesland',
    'strasse_hash',
    'email_domain',
    'telefon_land_code',
    'arbeitgeber_code',
    'branche_code',
    'beschaeftigungsstatus',
    'einkommensklasse',
    'familienstand',
    'anzahl_kinder',
    'wohnstatus',
    'immobilienbesitz_flag',
    'steuer_id_hash',
    'ausweis_typ',
    'ausweis_gueltig_bis',
    'vertragsbeginn',
    'vertragsende',
    'tarif_code',
    'kundenwert_segment',
    'cross_sell_score',
    'churn_score',
    'kampagne_code',
    'bemerkung',
    'payload_hash',
]

STRING_OPTIONS = {
    "src_system": ["CORE_BANKING", "CRM", "ONLINE_BANKING", "LOAN_SYS"],
    "raw_event_type": ["INSERT", "UPDATE", "INVALIDATE", "DELETE"],
    "konto_typ": ["GIRO", "SPAR", "KREDIT", "DEPOT"],
    "konto_art": ["PRIVAT", "GESCHAEFT", "GEMEINSCHAFT"],
    "produkt_name": ["Giro Basic", "Giro Premium", "Tagesgeld", "Kredit Flex"],
    "produkt_gruppe": ["GIRO", "SAVINGS", "LOAN", "CARD"],
    "produkt_kategorie": ["STANDARD", "PREMIUM", "STUDENT", "BUSINESS"],
    "konto_status": ["AKTIV", "ACTIVE", "GESPERRT", "BLOCKED", "GEKUENDIGT", "CLOSED", "PENDING"],
    "waehrung_code": ["EUR", "USD", "CHF", "GBP", "XYZ"],
    "branch_region": ["NORD", "SUED", "WEST", "OST", "MITTE"],
    "kunden_typ": ["PRIVATE", "BUSINESS"],
    "kunden_segment": ["VIP", "PRIVATE_BANKING", "PREMIUM", "AFFLUENT", "STANDARD", "BASIC", "JUNIOR", "STUDENT"],
    "kunden_subsegment": ["A", "B", "C", "D"],
    "aufenthaltsstaat": ["DE", "AT", "CH", "NL", "FR"],
    "steuer_land": ["DE", "AT", "CH", "NL", "FR"],
    "nationalitaet": ["DE", "AT", "CH", "TR", "SY", "ES"],
    "kyc_status": ["VOLLSTAENDIG", "COMPLETE", "APPROVED", "OFFEN", "OPEN", "MISSING", "REVIEW", "REJECTED"],
    "aml_status": ["OK", "REVIEW", "BLOCKED"],
    "vertriebs_kanal": ["BRANCH", "ONLINE", "MOBILE", "CALLCENTER"],
    "rating_code": ["AAA", "AA", "A", "BBB", "BB", "B"],
    "scoring_modell": ["M1", "M2", "M3"],
    "fraud_status": ["OK", "REVIEW", "BLOCKED"],
    "email_domain": ["example.de", "mail.de", "testbank.de", "demo.com"],
    "telefon_land_code": ["+49", "+43", "+41", "+31"],
    "branche_code": ["FIN", "IT", "RET", "MED", "EDU"],
    "beschaeftigungsstatus": ["ANGESTELLT", "SELBSTAENDIG", "STUDENT", "RENTNER"],
    "einkommensklasse": ["LOW", "MEDIUM", "HIGH"],
    "familienstand": ["LEDIG", "VERHEIRATET", "GESCHIEDEN"],
    "wohnstatus": ["MIETE", "EIGENTUM", "FAMILIE"],
    "ausweis_typ": ["PA", "PASS", "AUFENTHALTSTITEL"],
    "tarif_code": ["T001", "T002", "T003", "T004"],
    "kundenwert_segment": ["LOW", "MEDIUM", "HIGH", "TOP"],
}

DATE_BASE = date(2024, 1, 1)
TS_BASE = datetime(2024, 1, 1, 0, 0, 0)


def random_decimal(min_value: int, max_value: int, scale: int = 2) -> Decimal:
    value = random.uniform(min_value, max_value)
    return Decimal(str(round(value, scale)))


def stable_hash(value: str, length: int = 32) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def make_row(i: int, unique_accounts: int) -> dict[str, Any]:
    account_seq = i % unique_accounts
    customer_seq = account_seq // 2
    day_offset = i % 730

    idh_gltg_fach_adtm = DATE_BASE + timedelta(days=day_offset % 365)
    is_closed = i % 17 == 0
    idh_gltg_fach_edtm = idh_gltg_fach_adtm + timedelta(days=random.randint(30, 500)) if is_closed else date(9999, 12, 31)
    trans_start = TS_BASE + timedelta(days=day_offset, seconds=i % 86400)

    konto_status = random.choice(STRING_OPTIONS["konto_status"])
    if is_closed:
        konto_status = random.choice(["GEKUENDIGT", "CLOSED"])

    iban = f"DE{account_seq % 89:02d}10000000{account_seq:010d}"
    produkt_code = random.randint(1, 9999)

    return {
        "src_system": random.choice(STRING_OPTIONS["src_system"]),
        "raw_file_name": f"kunden_konto_feed_{i // 10000:05d}.csv",
        "raw_event_type": random.choice(STRING_OPTIONS["raw_event_type"]),
        "load_batch_id": f"BATCH_{i // 10000:06d}",
        "source_row_id": i + 1,
        "ingestion_ts": trans_start + timedelta(minutes=5),
        "kunden_nr": f"K{customer_seq:010d}",
        "konto_nr": f"A{account_seq:012d}",
        "iban": iban,
        "konto_typ": random.choice(STRING_OPTIONS["konto_typ"]),
        "konto_art": random.choice(STRING_OPTIONS["konto_art"]),
        "produkt_code": produkt_code,
        "produkt_name": random.choice(STRING_OPTIONS["produkt_name"]),
        "produkt_gruppe": random.choice(STRING_OPTIONS["produkt_gruppe"]),
        "produkt_kategorie": random.choice(STRING_OPTIONS["produkt_kategorie"]),
        "konto_status": konto_status,
        "waehrung_code": random.choice(STRING_OPTIONS["waehrung_code"]),
        "branch_code": f"BR{random.randint(1, 99):03d}",
        "branch_name": f"BRANCH_{random.randint(1, 30):03d}",
        "branch_region": random.choice(STRING_OPTIONS["branch_region"]),
        "kunden_typ": random.choice(STRING_OPTIONS["kunden_typ"]),
        "kunden_segment": random.choice(STRING_OPTIONS["kunden_segment"]),
        "kunden_subsegment": random.choice(STRING_OPTIONS["kunden_subsegment"]),
        "aufenthaltsstaat": random.choice(STRING_OPTIONS["aufenthaltsstaat"]),
        "steuer_land": random.choice(STRING_OPTIONS["steuer_land"]),
        "nationalitaet": random.choice(STRING_OPTIONS["nationalitaet"]),
        "risiko_klasse": random.randint(1, 5),
        "kyc_status": random.choice(STRING_OPTIONS["kyc_status"]),
        "aml_status": random.choice(STRING_OPTIONS["aml_status"]),
        "pep_flag": i % 97 == 0,
        "berater_id": f"B{random.randint(1, 500):05d}",
        "berater_team": f"TEAM_{random.randint(1, 20):02d}",
        "vertriebs_kanal": random.choice(STRING_OPTIONS["vertriebs_kanal"]),
        "idh_gltg_fach_adtm": idh_gltg_fach_adtm,
        "idh_gltg_fach_edtm": idh_gltg_fach_edtm,
        "trans_start": trans_start,
        "buchungstag": idh_gltg_fach_adtm,
        "aktuell_saldo": random_decimal(-5000, 150000),
        "kredit_limit": random_decimal(0, 100000),
        "zins_rate": random_decimal(0, 10, 6),
        "ueberziehung_erlaubt": i % 3 == 0,
        "monatlicher_eingang": random_decimal(0, 25000),
        "monatlicher_ausgang": random_decimal(0, 25000),
        "saldo_vormonat": random_decimal(-5000, 150000),
        "dispo_limit": random_decimal(0, 20000),
        "gebuehren_monat": random_decimal(0, 100),
        "offene_posten_anzahl": random.randint(0, 20),
        "letzte_transaktion_ts": trans_start + timedelta(hours=random.randint(0, 240)),
        "letzte_transaktion_betrag": random_decimal(-10000, 10000),
        "anzahl_transaktionen_30t": random.randint(0, 250),
        "anzahl_transaktionen_90t": random.randint(0, 750),
        "kredit_score": random.randint(250, 950),
        "rating_code": random.choice(STRING_OPTIONS["rating_code"]),
        "scoring_modell": random.choice(STRING_OPTIONS["scoring_modell"]),
        "default_wahrscheinlichkeit": random_decimal(0, 1, 6),
        "sicherheiten_wert": random_decimal(0, 500000),
        "beleihungsquote": random_decimal(0, 1, 6),
        "fraud_score": random.randint(0, 1000),
        "fraud_status": random.choice(STRING_OPTIONS["fraud_status"]),
        "consent_marketing": i % 2 == 0,
        "consent_onlinebanking": i % 5 != 0,
        "digital_aktiv": i % 4 != 0,
        "app_nutzung_30t": random.randint(0, 120),
        "online_login_30t": random.randint(0, 90),
        "papierlos_flag": i % 2 == 0,
        "kontostand_warnung_flag": i % 7 == 0,
        "letzter_login_ts": trans_start + timedelta(days=random.randint(0, 30)),
        "iban_valid_flag": True,
        "adresse_valid_flag": i % 101 != 0,
        "email_valid_flag": i % 103 != 0,
        "telefon_valid_flag": i % 107 != 0,
        "geburtsdatum": date(1940 + (i % 65), 1 + (i % 12), 1 + (i % 28)),
        "kunden_alter": 18 + (i % 65),
        "plz": f"{10000 + (i % 89999)}",
        "ort": f"ORT_{i % 250:03d}",
        "bundesland": random.choice(["NI", "NW", "BY", "BW", "HE", "HH", "BE"]),
        "strasse_hash": stable_hash(f"street-{i}"),
        "email_domain": random.choice(STRING_OPTIONS["email_domain"]),
        "telefon_land_code": random.choice(STRING_OPTIONS["telefon_land_code"]),
        "arbeitgeber_code": f"AG{i % 5000:05d}",
        "branche_code": random.choice(STRING_OPTIONS["branche_code"]),
        "beschaeftigungsstatus": random.choice(STRING_OPTIONS["beschaeftigungsstatus"]),
        "einkommensklasse": random.choice(STRING_OPTIONS["einkommensklasse"]),
        "familienstand": random.choice(STRING_OPTIONS["familienstand"]),
        "anzahl_kinder": random.randint(0, 5),
        "wohnstatus": random.choice(STRING_OPTIONS["wohnstatus"]),
        "immobilienbesitz_flag": i % 6 == 0,
        "steuer_id_hash": stable_hash(f"tax-{i}"),
        "ausweis_typ": random.choice(STRING_OPTIONS["ausweis_typ"]),
        "ausweis_gueltig_bis": DATE_BASE + timedelta(days=365 + (i % 3650)),
        "vertragsbeginn": DATE_BASE - timedelta(days=i % 3650),
        "vertragsende": date(9999, 12, 31) if i % 13 != 0 else DATE_BASE + timedelta(days=i % 365),
        "tarif_code": random.choice(STRING_OPTIONS["tarif_code"]),
        "kundenwert_segment": random.choice(STRING_OPTIONS["kundenwert_segment"]),
        "cross_sell_score": random.randint(0, 1000),
        "churn_score": random.randint(0, 1000),
        "kampagne_code": f"CMP{i % 100:03d}",
        "bemerkung": f"synthetic benchmark row {i}",
        "payload_hash": stable_hash(f"payload-{i}-{account_seq}"),
    }


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return "timestamp '" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    if isinstance(value, date):
        return "date '" + value.isoformat() + "'"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def chunks(iterable: Iterable[dict[str, Any]], size: int):
    batch = []
    for row in iterable:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def row_generator(rows: int, duplicate_ratio: float):
    unique_accounts = max(1, int(rows * (1 - duplicate_ratio)))
    for i in range(rows):
        yield make_row(i, unique_accounts)


def insert_batch(cursor, full_table_name: str, batch: list[dict[str, Any]]) -> None:
    col_list = ", ".join(COLUMNS)
    values = []
    for row in batch:
        row_values = ", ".join(sql_literal(row[col]) for col in COLUMNS)
        values.append(f"({row_values})")
    sql = f"INSERT INTO {full_table_name} ({col_list}) VALUES\n" + ",\n".join(values)
    cursor.execute(sql)


def main() -> None:
    TRINO_HOST = os.getenv("TRINO_HOST", "fi-starburst.entw.dapcont.intern")
    TRINO_PORT = int(os.getenv("TRINO_PORT", "443"))
    TRINO_USER = os.getenv("TRINO_USER", "J548910@v990dtv1.v990.intern")
    TRINO_CATALOG = os.getenv("TRINO_CATALOG", '"lakekeeper-etaps2"')
    TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", '"j548910"')
    SOURCE_TABLE = os.getenv("SOURCE_TABLE", '"bronze_kunden_konto_feed_100"')
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--duplicate-ratio", type=float, default=0.0)
    parser.add_argument("--clear-table", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{SOURCE_TABLE}"

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="https",
        auth=OAuth2Authentication(),
        verify="/Users/j548910/.dbt/cert.pem"
    )

    cur = conn.cursor()

    if args.clear_table:
        print(f"Clearing {full_table_name} ...")
        cur.execute(f"DELETE FROM {full_table_name}")

    inserted = 0
    for batch in chunks(row_generator(args.rows, args.duplicate_ratio), args.batch_size):
        insert_batch(cur, full_table_name, batch)
        inserted += len(batch)
        print(f"Inserted {inserted}/{args.rows} rows")

    print(f"Done. Inserted {inserted} rows into {full_table_name}.")


if __name__ == "__main__":
    main()

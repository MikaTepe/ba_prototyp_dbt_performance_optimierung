from datetime import date, timedelta
from decimal import Decimal
import os
import random
import trino
from trino.auth import OAuth2Authentication


# -----------------------------
# Trino / Starburst connection
# -----------------------------

TRINO_HOST = os.getenv("TRINO_HOST", "fi-starburst.entw.dapcont.intern")
TRINO_PORT = int(os.getenv("TRINO_PORT", "443"))
TRINO_USER = os.getenv("TRINO_USER", "J548910@v990dtv1.v990.intern")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", '"lakekeeper-etaps2"')
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", '"j548910"')
SOURCE_TABLE = os.getenv("SOURCE_TABLE", '"bronze_kunden_konto_feed_1"')


# Set to True if you want to clear the source table before loading mock data
CLEAR_TABLE_BEFORE_INSERT = False

# Number of mock rows
ROW_COUNT = 1000000
BATCH_SIZE = 2000


def sql_string(value: str) -> str:
    """Safely format a string value for SQL."""
    if value is None:
        return "null"
    return "'" + value.replace("'", "''") + "'"


def sql_date(value: date) -> str:
    """Format a Python date as a Trino DATE literal."""
    if value is None:
        return "null"
    return f"DATE '{value.isoformat()}'"


def sql_decimal(value: Decimal) -> str:
    """Format decimal value for SQL."""
    if value is None:
        return "null"
    return str(value)


def sql_bool(value: bool) -> str:
    """Format boolean value for SQL."""
    if value is None:
        return "null"
    return "true" if value else "false"


def generate_mock_rows(row_count: int = 10000) -> list[dict]:
    random.seed(42)

    produkt_options = [
        (1001, "Girokonto"),
        (1002, "Tagesgeldkonto"),
        (1003, "Kreditkarte"),
        (1004, "Depotkonto"),
        (1005, "Sparkonto"),
    ]

    konto_status_options = ["AKTIV", "GESPERRT", "GEKUENDIGT", "IN_PRUEFUNG"]
    waehrung_options = ["EUR", "USD", "GBP"]
    kunden_typ_options = ["PRIVAT", "GESCHAEFT"]
    kunden_segment_options = ["STANDARD", "PREMIUM", "VIP", "JUNIOR"]
    staat_options = ["DE", "AT", "CH", "FR", "NL"]
    kyc_status_options = ["VOLLSTAENDIG", "OFFEN", "ABGELEHNT", "IN_PRUEFUNG"]
    raw_event_options = ["FULL_LOAD", "DELTA_LOAD", "CORRECTION"]

    rows = []

    base_date = date(2026, 4, 1)

    for i in range(1, row_count + 1):
        kunden_nr = 100000 + i
        konto_nr = 90000000 + i

        produkt_code, produkt_name = random.choice(produkt_options)

        fach_sdat = base_date + timedelta(days=random.randint(0, 10))
        fach_edat = date(9999, 12, 31)
        trans_start = fach_sdat + timedelta(days=random.randint(0, 3))

        aktuell_saldo = Decimal(random.randint(-5000, 50000)) / Decimal("1.00")
        kredit_limit = Decimal(random.choice([0, 500, 1000, 2500, 5000, 10000]))
        zins_rate = Decimal(str(round(random.uniform(0.000000, 0.085000), 6)))

        row = {
            "src_system": "CORE_BANKING",
            "raw_file_name": f"account_feed_202604{i % 30 + 1:02d}.csv",
            "raw_event_type": random.choice(raw_event_options),

            "kunden_nr": kunden_nr,
            "konto_nr": konto_nr,
            "iban": f"DE{random.randint(10, 99)}1000000000{konto_nr}",

            "produkt_code": produkt_code,
            "produkt_name": produkt_name,
            "konto_status": random.choice(konto_status_options),
            "waehrung_code": random.choice(waehrung_options),
            "branch_code": f"BR{random.randint(1, 20):03d}",

            "kunden_typ": random.choice(kunden_typ_options),
            "kunden_segment": random.choice(kunden_segment_options),
            "aufenthaltsstaat": random.choice(staat_options),

            "risiko_klasse": random.randint(1, 5),
            "kyc_status": random.choice(kyc_status_options),
            "berater_id": 7000 + random.randint(1, 50),

            "fach_sdat": fach_sdat,
            "fach_edat": fach_edat,
            "trans_start": trans_start,

            "aktuell_saldo": aktuell_saldo,
            "kredit_limit": kredit_limit,
            "zins_rate": zins_rate,
            "ueberziehung_erlaubt": random.choice([True, False]),
        }

        rows.append(row)

    return rows


def row_to_sql_values(row: dict) -> str:
    return f"""(
        {sql_string(row["src_system"])},
        {sql_string(row["raw_file_name"])},
        {sql_string(row["raw_event_type"])},

        {row["kunden_nr"]},
        {row["konto_nr"]},
        {sql_string(row["iban"])},

        {row["produkt_code"]},
        {sql_string(row["produkt_name"])},
        {sql_string(row["konto_status"])},
        {sql_string(row["waehrung_code"])},
        {sql_string(row["branch_code"])},

        {sql_string(row["kunden_typ"])},
        {sql_string(row["kunden_segment"])},
        {sql_string(row["aufenthaltsstaat"])},

        {row["risiko_klasse"]},
        {sql_string(row["kyc_status"])},
        {row["berater_id"]},

        {sql_date(row["fach_sdat"])},
        {sql_date(row["fach_edat"])},
        {sql_date(row["trans_start"])},

        {sql_decimal(row["aktuell_saldo"])},
        {sql_decimal(row["kredit_limit"])},
        {sql_decimal(row["zins_rate"])},
        {sql_bool(row["ueberziehung_erlaubt"])}
    )"""


def build_insert_sql(full_table_name: str, rows: list[dict]) -> str:
    values_sql = ",\n".join(row_to_sql_values(row) for row in rows)

    return f"""
insert into {full_table_name} (
    src_system,
    raw_file_name,
    raw_event_type,

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

    idh_gltg_fach_adtm,
    idh_gltg_fach_edtm,
    trans_start,

    aktuell_saldo,
    kredit_limit,
    zins_rate,
    ueberziehung_erlaubt
)
values
{values_sql}
"""


def main():
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

    cursor = conn.cursor()

    if CLEAR_TABLE_BEFORE_INSERT:
        print(f"Clearing table: {full_table_name}")
        cursor.execute(f"delete from {full_table_name}")

    rows = generate_mock_rows(ROW_COUNT)

    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch = rows[batch_start:batch_start + BATCH_SIZE]
        insert_sql = build_insert_sql(full_table_name, batch)

        print(f"Inserting rows {batch_start + 1} to {batch_start + len(batch)}")
        cursor.execute(insert_sql)

    print(f"Finished loading {len(rows)} mock rows into {full_table_name}")


if __name__ == "__main__":
    main()
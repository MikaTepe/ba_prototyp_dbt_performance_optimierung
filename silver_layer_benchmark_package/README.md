# Silver-layer benchmark package

This package contains a Trino/Starburst-compatible benchmark setup for testing column-count effects in a dbt-based silver-layer transformation framework.

## Files

- `sql/00_source_100_columns_ddl.sql`  
  Creates the 100-column bronze source table `local_lakehouse.bronze.bronze_kunden_konto_feed_100`.

- `sql/kunden_konto_unitemporal_30_ddl.sql`  
- `sql/kunden_konto_unitemporal_60_ddl.sql`  
- `sql/kunden_konto_unitemporal_100_ddl.sql`  
- `sql/kunden_konto_bitemporal_30_ddl.sql`  
- `sql/kunden_konto_bitemporal_60_ddl.sql`  
- `sql/kunden_konto_bitemporal_100_ddl.sql`  
  Creates six target tables with exactly 30, 60, and 100 columns per history pattern.

- `sql/01_all_target_tables_ddl.sql`  
  Combined DDL for all six target tables.

- `python/fill_source_bronze_kunden_konto_feed_100.py`  
  Generates synthetic source rows and inserts them into the 100-column source table.

- `dbt_sources/sources.yml`  
  Example dbt source definition.

- `dbt_models/*.sql`  
  Six dbt/Trino-compatible model SQL files. They can be used as snapshot input SQL or adapted into your custom snapshot/materialization.

## Column-count design

The target tables use exact total column counts, including technical columns:

| Pattern | Table | Total columns | Technical columns | Business columns |
|---|---|---:|---:|---:|
| Unitemporal | `kunden_konto_unitemporal_30` | 30 | 8 | 22 |
| Unitemporal | `kunden_konto_unitemporal_60` | 60 | 8 | 52 |
| Unitemporal | `kunden_konto_unitemporal_100` | 100 | 8 | 92 |
| Bitemporal | `kunden_konto_bitemporal_30` | 30 | 10 | 20 |
| Bitemporal | `kunden_konto_bitemporal_60` | 60 | 10 | 50 |
| Bitemporal | `kunden_konto_bitemporal_100` | 100 | 10 | 90 |

## Usage

1. Run the source DDL.
2. Run the target DDLs.
3. Add the source YAML to your dbt project.
4. Add the dbt model SQL files to your dbt project or adapt their SELECT logic into your snapshot setup.
5. Fill the source table:

```bash
pip install trino
python python/fill_source_bronze_kunden_konto_feed_100.py \
  --host localhost \
  --port 8080 \
  --user trino \
  --http-scheme http \
  --catalog local_lakehouse \
  --schema bronze \
  --table bronze_kunden_konto_feed_100 \
  --rows 850000 \
  --batch-size 500 \
  --clear-table
```

## Notes

- All timestamp columns use `timestamp(6)`.
- The dbt SQL files intentionally avoid `SELECT *` in the final output so the selected target width is explicit.
- The source table contains 100 columns, while each target table has an exact target width of 30, 60, or 100 columns.
- The SQL normalizes several business columns and keeps the deduplication pattern from your existing example.
- I fixed the `src_system` condition from the example to `src_system is null or src_system = ''`; the original condition would turn nearly every non-null value into `UNKNOWN`.

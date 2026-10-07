import subprocess
from datetime import datetime
from time import perf_counter
import os
import trino
from trino.auth import OAuth2Authentication


NUMBER_OF_RUNS = 10

´STOP_ON_FAILURE = True

TRINO_HOST = os.getenv("TRINO_HOST", "fi-starburst.entw.dapcont.intern")
TRINO_PORT = int(os.getenv("TRINO_PORT", "443"))
TRINO_USER = os.getenv("TRINO_USER", "J548910@v990dtv1.v990.intern")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", '"lakekeeper-etaps2"')
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", '"j548910"')

´DBT_COMMANDS = {"100 INSERT": [
    "dbt",
    "snapshot",
    "--select",
    "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_100",
    "--vars",
    '{"INR_FKEY": "j548910", "TAB_FKEY": "silver_kunden_konto_unitemporal_100", "BDAT": "2025-05-18", "ACTIVE_SNAPSHOT": "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_100"}',
    "--debug"
], 
                "60 INSERT":[
    "dbt",
    "snapshot",
    "--select",
    "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_60",
    "--vars",
    '{"INR_FKEY": "j548910", "TAB_FKEY": "silver_kunden_konto_unitemporal_60", "BDAT": "2025-05-18", "ACTIVE_SNAPSHOT": "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_60"}',
    "--debug"
],
            "100 UPDATE": [
    "dbt",
    "snapshot",
    "--select",
    "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_100",
    "--vars",
    '{"INR_FKEY": "j548910", "TAB_FKEY": "silver_kunden_konto_unitemporal_100", "BDAT": "2025-06-12", "ACTIVE_SNAPSHOT": "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_100"}',
    "--debug"
],
                "60 UPDATE":[
    "dbt",
    "snapshot",
    "--select",
    "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_60",
    "--vars",
    '{"INR_FKEY": "j548910", "TAB_FKEY": "silver_kunden_konto_unitemporal_60", "BDAT": "2025-06-12", "ACTIVE_SNAPSHOT": "INSERT_SILVER_KUNDEN_KONTO_UNITEMPORAL_60"}',
    "--debug"
]}


run_results = []

for run_type in DBT_COMMANDS:
    if "UPDATE" in run_type:
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
        if run_type == "100 UPDATE":
            backup_full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_100_backup"
            full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_100"
            cursor.execute(f"insert into {backup_full_table_name} select * from {full_table_name}")
        elif run_type == "60 UPDATE":
            backup_full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_60_backup"
            full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_60"
            cursor.execute(f"insert into {backup_full_table_name} select * from {full_table_name}")



        
        cursor.execute(f"update {TRINO_CATALOG}.{TRINO_SCHEMA}.bronze_kunden_konto_feed_100 set konto_status = 'ACTIVE', idh_gltg_fach_adtm = current_date where konto_status = 'GESPERRT'")
    

        
    dbt_command = DBT_COMMANDS[run_type]
    for run_number in range(1, NUMBER_OF_RUNS + 1):
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
        if run_type == "100 INSERT":
            full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_100"
            cursor.execute(f"truncate table {full_table_name}")
        elif run_type == "60 INSERT":
            full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_60"
            cursor.execute(f"truncate table {full_table_name}")
        print("\n" + "=" * 60)
        print(f"Starting dbt run {run_number}/{NUMBER_OF_RUNS}")
        print("=" * 60)

        start_datetime = datetime.now()
        start_counter = perf_counter()

        print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")


        log_file = f"performance_analysis/dbt_run_log_{run_type}_{run_number}.txt"

        with open(log_file, "a", encoding="utf-8") as f:
            result = subprocess.run(
                dbt_command,
                stdout=f,
                stderr=f,
                text=True
            )

        end_counter = perf_counter()
        end_datetime = datetime.now()

        runtime_seconds = end_counter - start_counter

        print(f"End time:   {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Runtime:    {runtime_seconds:.2f} seconds")

        run_result = {
            "run_number": run_number,
            "start_time": start_datetime,
            "end_time": end_datetime,
            "runtime_seconds": runtime_seconds,
            "return_code": result.returncode,
            "success": result.returncode == 0
        }

        run_results.append(run_result)

        if result.returncode == 0:
            print(f"dbt run {run_number}/{NUMBER_OF_RUNS} completed successfully.")
            if run_type == "100 UPDATE":
                backup_full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_100_backup"
                full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_100"
                cursor.execute(f"truncate table {full_table_name}")
                cursor.execute(f"insert into {full_table_name} select * from {backup_full_table_name}")
            elif run_type == "60 UPDATE":
                backup_full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_60_backup"
                full_table_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.silver_kunden_konto_unitemporal_60"
                cursor.execute(f"truncate table {full_table_name}")
                cursor.execute(f"insert into {full_table_name} select * from {backup_full_table_name}")
        else:
            print(f"dbt run {run_number}/{NUMBER_OF_RUNS} failed with return code {result.returncode}.")

            if STOP_ON_FAILURE:
                print("Stopping execution because STOP_ON_FAILURE is enabled.")
                break


    completed_runs = len(run_results)

    if completed_runs > 0:
        total_runtime = sum(run["runtime_seconds"] for run in run_results)
        average_runtime = total_runtime / completed_runs

        print("\n" + "=" * 60)
        print("Runtime Summary")
        print("=" * 60)

        for run in run_results:
            status = "SUCCESS" if run["success"] else "FAILED"

            print(
                f"Run {run['run_number']:>2}: "
                f"{run['runtime_seconds']:.2f} seconds | "
                f"{status} | "
                f"{run['start_time'].strftime('%H:%M:%S')} - "
                f"{run['end_time'].strftime('%H:%M:%S')}"
            )

        print("-" * 60)
        print(f"Completed runs:  {completed_runs}")
        print(f"Total runtime:    {total_runtime:.2f} seconds")
        print(f"Average runtime:  {average_runtime:.2f} seconds")
    else:
        print("No dbt runs were executed.")
    run_results = []
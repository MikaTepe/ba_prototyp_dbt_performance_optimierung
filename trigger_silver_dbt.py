# trigger_silver_dbt.py

import argparse
import json
import shlex
import subprocess
import sys


def trigger_dbt(insert_statement: str, inr_fkey: str, bdat: str, debug: bool) -> int:
    dbt_vars = {
        "INR_FKEY": inr_fkey
    }

    if bdat:
        dbt_vars["bdat"] = bdat

    command = [
        "dbt",
        "run",
        "--select",
        insert_statement,
        "--vars",
        json.dumps(dbt_vars)
    ]

    if debug:
        command.append("--debug")

    print("Running dbt command:")
    print(shlex.join(command))
    print()

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if debug and result.stdout:
        print("dbt output:")
        print(result.stdout)

    if result.returncode == 0:
        print("dbt operation erfolgreich abgeschlossen.")
        print("\ndbt output:")
        print(result.stdout)
    else:
        print("dbt operation fehlgeschlagen.")
        print(f"Return code: {result.returncode}")

        if result.stderr:
            print("\nError message:")
            print(result.stderr)

        if result.stdout:
            print("\ndbt output:")
            print(result.stdout)

    return result.returncode


def main():
    parser = argparse.ArgumentParser(
        description="Trigger the Silver Layer dbt prototype."
    )

    parser.add_argument(
        "--insert-statement",
        required=True,
        help="Name of the dbt model to run."
    )

    parser.add_argument(
        "--inr-fkey",
        required=True,
        help="Name of the model target schema."
    )

    parser.add_argument(
        "--bdat",
        required=False,
        help="Optional booking date."
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show dbt debug logs in the terminal."
    )

    args = parser.parse_args()

    return_code = trigger_dbt(
        insert_statement=args.insert_statement,
        inr_fkey=args.inr_fkey,
        bdat=args.bdat,
        debug=args.debug
    )

    sys.exit(return_code)


if __name__ == "__main__":
    main()
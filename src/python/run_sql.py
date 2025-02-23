import argparse
import duckdb
import os
import os.path
from datetime import date
import re


def run() -> None:
    """
    Runs the query in the file passed on the command line.
    """
    parser = argparse.ArgumentParser(
        prog="sql_runner",
        description=run.__doc__,
    )
    parser.add_argument("-f", "--file", help="the file of the .sql file to run")
    parser.add_argument(
        "-s",
        "--start",
        type=int,
        default=date.min.year,
        help="the first year of data to read. Accesible in SQL file as {{ start }}",
    )
    parser.add_argument(
        "-e",
        "--end",
        type=int,
        default=date.max.year,
        help="the last year of data to read. Accesible in SQL file as {{ end }}",
    )
    args = parser.parse_args()

    swap_dir = os.path.join("tmp", "duckdb_swap")
    os.makedirs(swap_dir, exist_ok=True)

    options = {
        "temp_directory": swap_dir,
    }
    for var_name, value in options.items():
        duckdb.sql(f"set {var_name} = '{value}';")

    os.environ["MALLOC_CONF"] = (
        f"narenas:{os.cpu_count()},lg_chunk:21,background_thread:true,"
        + "dirty_decay_ms:10000,muzzy_decay_ms:10000"
    )

    with open(args.file, "r") as query:
        for statement in query.read().split(";"):
            for var in ("start", "end"):
                statement = re.sub(
                    r"\{\{\s*" + re.escape(var) + r"\s*\}\}",
                    str(getattr(args, var)),
                    statement,
                )

            res = duckdb.sql(statement)
            if res is not None:
                res.show()

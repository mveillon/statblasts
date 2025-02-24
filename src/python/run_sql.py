import argparse
import duckdb
import os
import os.path
from datetime import date
import re


def run_sql(path: str, start: int, end: int) -> None:
    """
    Runs the SQL queries in the given file.

    Parameters:
        path (str): the path of the .sql file to run
        start (int): the first year to process
        end (int): the last year to process

    Returns:
        None
    """
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

    with open(path, "r") as query:
        for statement in query.read().split(";"):
            for var_name, var_val in [("start", start), ("end", end)]:
                statement = re.sub(
                    r"\{\{\s*" + re.escape(var_name) + r"\s*\}\}",
                    str(var_val),
                    statement,
                )

            res = duckdb.sql(statement)
            if res is not None:
                res.show()

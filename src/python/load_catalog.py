import os
import os.path
import glob
from datetime import date
from typing import Literal

from src.python.run_sql import run_file, run_query


def load_catalog(
    catalog: Literal["build", "publish"], start: int, end: int, step: int = 0
) -> None:
    """
    Generates all the tables in the given folder.

    Parameters:
        catalog (str): whether to load build or publish
        start (int): the first year to process
        end (int): the last day to process
        step (int): the number of years per batch to process. If zero, the default,
            everything will be done in one batch

    Returns:
        None
    """
    ddl_files = glob.glob(os.path.join("src", "ddl", catalog, "*.sql"))
    for path in ddl_files:
        run_query(
            "drop table if exists "
            + f"{catalog}.{os.path.splitext(os.path.basename(path))[0]};"
        )
        run_file(path)

    dml_files = glob.glob(os.path.join("src", "dml", catalog, "*.sql"))

    current = start
    end = min(date.today().year, end)
    while current <= end:
        if step == 0:
            batch_end = end
        else:
            batch_end = current + step - 1

        print(f"Processing {current} to {batch_end}.")

        for path in dml_files:
            run_file(path, subs={"start": current, "end": batch_end})

        current = batch_end + 1

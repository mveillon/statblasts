import os
import os.path
import glob
import shutil
from datetime import date

from src.python.run_sql import run_sql


def generate_build(start: int, end: int, step: int = 10) -> None:
    """
    Generates all the tables in the build folder.

    Parameters:
        start (int): the first year to process
        end (int): the last day to process
        step (int): the number of years per batch to process. Default is 10

    Returns:
        None
    """
    target_dir = os.path.join("data", "build")
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)

    os.makedirs(target_dir, exist_ok=True)
    sql_files = glob.glob(os.path.join("src", "dml", "build", "*.sql"))

    current = start
    step = 10
    while current <= min(date.today().year, end):
        batch_end = current + step
        print(f"Processing {current} to {batch_end}.")

        for path in sql_files:
            print(f"\tRunning {path}.")
            run_sql(path, current, batch_end)

        current = batch_end + 1



    
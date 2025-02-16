import argparse
import duckdb


def run() -> None:
    """
    Runs the query in the file passed on the command line.
    """
    parser = argparse.ArgumentParser(
        prog="sql_runner",
        description=run.__doc__,
    )
    parser.add_argument("-f", "--file", help="the file of the .sql file to run")
    args = parser.parse_args()

    with open(args.file, "r") as query:
        res = duckdb.sql(query.read())
        if res is not None:
            res.show()

import argparse
from datetime import date

def parse_args() -> argparse.Namespace:
    """
    Parses the command line arguments.

    Parameters:
        None

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        prog="sql_runner",
    )

    subparsers = parser.add_subparsers(
        title="subcommands",
        description="valid subcommands",
        help="which command to run",
        dest="subparser_name",
    )

    run_file_parser = subparsers.add_parser("run", help="run a single SQL file")
    build_parser = subparsers.add_parser("build", help="generate tables in build")

    run_file_parser.add_argument("-f", "--file", help="the file of the .sql file to run")

    for subparser in (run_file_parser, build_parser):
        subparser.add_argument(
            "-s",
            "--start",
            type=int,
            default=date.min.year,
            help="the first year of data to read. Accesible in SQL file as {{ start }}",
        )
        subparser.add_argument(
            "-e",
            "--end",
            type=int,
            default=date.max.year,
            help="the last year of data to read. Accesible in SQL file as {{ end }}",
        )

    return parser.parse_args()

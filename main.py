from src.python.parse_args import parse_args
from src.python.run_sql import run_sql
from src.python.generate_build import generate_build


def run() -> None:
    """
    Parses the CLI args and executes the correct function.

    Parameters:
        None

    Returns:
        None
    """
    args = parse_args()
    if args.subparser_name == "run":
        run_sql(args.file, args.start, args.end)

    elif args.subparser_name == "build":
        generate_build(args.start, args.end)


if __name__ == "__main__":
    run()

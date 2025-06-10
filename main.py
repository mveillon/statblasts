from src.python.parse_args import parse_args
from src.python.run_sql import run_file
from src.python.generate_build import generate_build
from src.python.generate_publish import generate_publish


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
        run_file(args.file, subs={"start": args.start, "end": args.end})

    elif args.subparser_name == "build":
        generate_build(args.start, args.end)

    elif args.subparser_name == "publish":
        generate_publish(args.start, args.end)

    else:
        raise ValueError(f"Unknown subcommand: {args.subparser_name}")


if __name__ == "__main__":
    run()

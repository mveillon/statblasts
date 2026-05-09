from src.python.load_catalog import load_catalog


def generate_build(start: int, end: int, step: int = 0) -> None:
    """
    Loads all the tables in the build folder.

    Parameters:
        start (int): the first year to process
        end (int): the last day to process
        step (int): the number of years per batch to process. If zero, the default,
            everything will be done in one batch

    Returns:
        None
    """
    load_catalog("build", start, end, step=step)

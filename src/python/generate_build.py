from src.python.load_catalog import load_catalog

def generate_build(start: int, end: int, step: int = 10) -> None:
    """
    Loads all the tables in the build folder.

    Parameters:
        start (int): the first year to process
        end (int): the last day to process
        step (int): the number of years per batch to process. Default is 10

    Returns:
        None
    """
    load_catalog("build", start, end, step=step)

import duckdb
import os
import os.path
import re

from typing import Dict, Any, List


def run_query(query: str, subs: Dict[str, Any] = {}) -> None:
    """
    Runs a single query and returns the output, if there is one.

    Parameters:
        query (str): the query to run
        subs (Dict[str, Any]): dictionary containing substitutions to perform
            in the query

    Returns:
        out (Optional[Relation]): the output relation, or None if there is not one
    """
    swap_dir = os.path.join("tmp", "duckdb_swap")
    os.makedirs(swap_dir, exist_ok=True)

    options = {
        "temp_directory": swap_dir,
    }

    os.environ["MALLOC_CONF"] = (
        f"narenas:{os.cpu_count()},lg_chunk:21,background_thread:true,"
        + "dirty_decay_ms:10000,muzzy_decay_ms:10000"
    )

    for key, val in subs.items():
        query = re.sub(r"\{\{\s*" + re.escape(key) + r"\s*\}\}", str(val), query)

    with duckdb.connect(os.path.join("data", "output.db")) as duck:
        for var_name, value in options.items():
            duck.sql(f"set {var_name} = '{value}';")

        res = duck.sql(query)
        if res is not None:
            res.show()


def run_file(path: str, subs: Dict[str, Any] = {}) -> List[duckdb.DuckDBPyRelation]:
    """
    Runs the file passed to `path`.

    Parameters:
        path (str): the path to the .sql file to run
        subs (Dict[str, Any]): dictionary containing substitutions to perform
            in the query

    Returns:
        out (List[Relation]): a relation for each statment with a return value
    """
    out = []
    with open(path, "r") as query:
        print(f"Running {path}")
        for statement in query.read().split(";"):
            res = run_query(statement, subs=subs)
            if res is not None:
                out.append(res)

    return out

import logging
import pathlib
import sqlite3
from typing import Any, Dict

import pandas


def execute(input_file: pathlib.Path, output_dir: Dict[str, Any]) -> None:
    """
    Create the CSV dumps from SQLite
    :param input_file: SQLite file
    :param output_dir: Output directory
    """
    # Make output path
    output_dir["path"].mkdir(exist_ok=True)

    db = sqlite3.connect(str(input_file))
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    tables = cursor.fetchall()
    for table_name in tables:
        table_name = table_name[0]
        logging.info(f"Handling {table_name}")

        table = pandas.read_sql_query(f"SELECT * from {table_name}", db)
        table.to_csv(
            str(output_dir["path"].joinpath(table_name + ".csv")), index_label="index"
        )

    cursor.close()
    db.close()

    # Remove Schema
    output_dir["path"].joinpath("sqlite_sequence.csv").unlink()

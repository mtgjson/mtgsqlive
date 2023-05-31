import sqlite3
from typing import Any, Dict

import pandas as pd

from .abstract import AbstractConverter


class CsvConverter(AbstractConverter):
    sqlite_db: sqlite3.Connection

    def __init__(self, _: Dict[str, Any], output_dir: str) -> None:
        super().__init__({}, output_dir)

        db_path = self.output_obj.root_dir.joinpath("AllPrintings.sqlite")
        if not db_path.exists():
            raise FileNotFoundError()

        self.sqlite_db = sqlite3.connect(str(db_path))
        self.sqlite_db.text_factory = lambda x: str(x, "utf-8")

    def convert(self) -> None:
        cursor = self.sqlite_db.cursor()

        cursor.execute("SELECT `name` FROM `sqlite_master` WHERE `type`='table';")
        all_table_names = list(map("".join, cursor.fetchall()))
        cursor.close()

        for table_name in all_table_names:
            pd_table = pd.read_sql_query(
                f"SELECT * FROM `{table_name}`", self.sqlite_db
            )

            pd_table.to_csv(
                str(self.output_obj.root_dir.joinpath(f"{table_name}.csv")),
                encoding="utf-8",
            )

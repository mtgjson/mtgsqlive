import sqlite3
from typing import Any, Dict, Iterator

from .abstract import AbstractConverter


class SqliteBasedConverter(AbstractConverter):
    sqlite_db: sqlite3.Connection

    def __init__(self, mtgjson_dict: Dict[str, Any], output_dir: str) -> None:
        super().__init__(mtgjson_dict, output_dir)

        db_path = self.output_obj.root_dir.joinpath("AllPrintings.sqlite")
        if not db_path.exists():
            raise FileNotFoundError()

        self.sqlite_db = sqlite3.connect(str(db_path))
        self.sqlite_db.text_factory = lambda x: str(x, "utf-8")

    def get_next_table_name(self) -> Iterator[str]:
        cursor = self.sqlite_db.cursor()

        cursor.execute("SELECT `name` FROM `sqlite_master` WHERE `type`='table';")
        all_table_names = list(map("".join, cursor.fetchall()))
        cursor.close()

        for table_name in all_table_names:
            yield table_name

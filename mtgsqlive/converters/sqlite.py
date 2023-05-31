import sqlite3
from collections import defaultdict
from typing import Any, Dict, Iterator

import pymysql

from .sqllike import SqlLikeConverter

nested_dict: Any = lambda: defaultdict(nested_dict)


class SqliteConverter(SqlLikeConverter):
    def __init__(self, mtgjson_data: Dict[str, Any], output_dir: str) -> None:
        super().__init__(mtgjson_data, output_dir)

        self.output_obj.fp = sqlite3.connect(
            self.output_obj.root_dir.joinpath("AllPrintings.sqlite")
        )
        self.output_obj.fp.execute("pragma journal_mode=wal;")

    def convert(self) -> None:
        sql_schema_as_dict = self._generate_sql_schema_dict()
        schema_query = self._convert_schema_dict_to_query(
            sql_schema_as_dict, engine="", primary_key_op=""
        )

        self.output_obj.fp.executescript(schema_query)

        insert_data_generator = self.generate_database_insert_statements()
        self.write_statements_to_file(insert_data_generator)

    def create_insert_statement_body(self, data: Dict[str, Any]) -> str:
        pre_processed_values = []
        for value in data.values():
            if value is None:
                pre_processed_values.append("NULL")
                continue

            if isinstance(value, list):
                statement = (
                    pymysql.converters.escape_string(", ".join(map(str, value)))
                    .replace('\\"', '""')
                    .replace("'", "''")
                )
                pre_processed_values.append(f'"{statement}"')
            elif isinstance(value, bool):
                pre_processed_values.append("1" if value else "0")
            else:
                statement = (
                    pymysql.converters.escape_string(str(value))
                    .replace('\\"', '""')
                    .replace("'", "''")
                )
                pre_processed_values.append(f'"{statement}"')

        return ", ".join(pre_processed_values)

    def write_statements_to_file(self, data_generator: Iterator[str]) -> None:
        cursor = self.output_obj.fp.cursor()
        statements = []

        for statement in data_generator:
            statements.append(statement)
            if len(statements) >= 1_000:
                cursor.executescript("\n".join(statements))
                self.output_obj.fp.commit()
                statements = []
        cursor.executescript("\n".join(statements))
        self.output_obj.fp.commit()

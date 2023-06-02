from datetime import datetime
from typing import Any, Dict, Iterator

import pymysql.converters

from .sqllike import SqlLikeConverter


class MysqlConverter(SqlLikeConverter):
    def __init__(self, mtgjson_data: Dict[str, Any], output_dir: str):
        super().__init__(mtgjson_data, output_dir)
        self.output_obj.fp = self.output_obj.root_dir.joinpath("AllPrintings.sql").open(
            "w", encoding="utf-8"
        )

    def convert(self) -> None:
        sql_schema_as_dict = self._generate_sql_schema_dict()
        schema_query = self._convert_schema_dict_to_query(
            sql_schema_as_dict,
            engine="ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            primary_key_op="INTEGER PRIMARY KEY AUTO_INCREMENT",
        )

        header = "\n".join(
            (
                "-- MTGSQLive Output File",
                f"-- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"-- MTGJSON Version: {self.get_version()}",
                "",
                "START TRANSACTION;",
                "SET names 'utf8mb4';",
                "",
                schema_query,
                "",
                "COMMIT;",
                "",
                "",
            )
        )
        self.output_obj.fp.write(header)

        insert_data_generator = self.generate_database_insert_statements()
        self.write_statements_to_file(insert_data_generator)

    def create_insert_statement_body(self, data: Dict[str, Any]) -> str:
        pre_processed_values = []
        for value in data.values():
            if value is None:
                pre_processed_values.append("NULL")
                continue

            if isinstance(value, list):
                pre_processed_values.append(
                    '"'
                    + pymysql.converters.escape_string(", ".join(map(str, value)))
                    + '"'
                )
            elif isinstance(value, bool):
                pre_processed_values.append("1" if value else "0")
            else:
                pre_processed_values.append(
                    '"' + pymysql.converters.escape_string(str(value)) + '"'
                )

        return ", ".join(pre_processed_values)

    def write_statements_to_file(self, data_generator: Iterator[str]) -> None:
        statements = []
        for statement in data_generator:
            statements.append(statement)
            if len(statements) >= 1_000:
                self.output_obj.fp.writelines(statements)
                statements = []
        self.output_obj.fp.writelines(statements)

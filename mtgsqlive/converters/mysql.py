from collections import defaultdict
from datetime import datetime
from typing import Optional, Any, Dict, Iterator

import humps
import pymysql.converters

from . import AbstractConverter

nested_dict = lambda: defaultdict(nested_dict)


class MysqlConverter(AbstractConverter):
    def __init__(self, mtgjson_data: Dict[str, Any], output_dir: str):
        super().__init__(mtgjson_data, output_dir)
        self.output_obj.fp = self.output_obj.dir.joinpath("AllPrintings.sql").open(
            "w", encoding="utf-8"
        )

    def convert(self) -> None:
        sql_schema_as_dict = self.generate_sql_schema()
        sql_schema = self.convert_dict_to_sql_query(sql_schema_as_dict)

        header = "\n".join(
            (
                "-- MTGSQLive Output File",
                f"-- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                f"-- MTGJSON Version: {self.get_version()}",
                "",
                "START TRANSACTION;",
                "SET names 'utf8mb4';",
                "",
                sql_schema,
                "",
                "COMMIT;",
                "",
            )
        )

        self.output_obj.fp.write(header)

        insert_statements = []
        for statement in self.generate_database_insert_statements():
            insert_statements.append(statement)
            if len(insert_statements) >= 1_000:
                self.output_obj.fp.writelines(insert_statements)
                insert_statements = []
        self.output_obj.fp.writelines(insert_statements)

    def generate_sql_schema(self) -> Dict[str, Any]:
        schema = nested_dict()

        self.add_meta_table_schema(schema)
        self.add_set_table_schema(schema)
        self.add_card_table_schema(schema)
        self.add_token_table_schema(schema)
        self.add_card_legalities_table_schema(schema)
        self.add_card_rulings_table_schema(schema)
        self.add_card_foreign_data_table_schema(schema)
        self.add_set_translation_table_schema(schema)

        return schema

    @staticmethod
    def add_meta_table_schema(schema: Dict[str, Any]) -> None:
        schema["meta"]["date"]["type"] = "DATE"
        schema["meta"]["version"]["type"] = "VARCHAR"

    def add_set_table_schema(self, schema: Dict[str, Any]) -> None:
        for set_code, set_data in self.mtgjson_data.get("data").items():
            for set_attribute, set_attribute_data in set_data.items():
                set_attribute = humps.decamelize(set_attribute)

                if set_attribute in self.skipable_set_keys:
                    continue

                if set_attribute in ["code"]:
                    schema["sets"][set_attribute][
                        "type"
                    ] = "VARCHAR (8) UNIQUE NOT NULL"
                    continue

                schema["sets"][set_attribute]["type"] = self.get_sql_type(
                    set_attribute_data
                )

    def get_card_like_schema(self, schema: Dict[str, Any], key_name: str) -> None:
        for set_code, set_data in self.mtgjson_data.get("data").items():
            for mtgjson_card in set_data.get(key_name):
                for card_attribute, card_attribute_data in mtgjson_card.items():
                    card_attribute = humps.decamelize(card_attribute)

                    if card_attribute in [
                        "converted_mana_cost",
                        "legalities",
                        "foreign_data",
                        "rulings",
                        "identifiers",
                    ]:
                        continue

                    if card_attribute in ["uuid"]:
                        schema["sets"][card_attribute]["type"] = "VARCHAR (36) NOT NULL"
                        continue

                    schema[key_name][card_attribute]["type"] = self.get_sql_type(
                        card_attribute_data
                    )

    def add_card_table_schema(self, schema: Dict[str, Any]) -> None:
        self.get_card_like_schema(schema, "cards")

    def add_token_table_schema(self, schema: Dict[str, Any]) -> None:
        self.get_card_like_schema(schema, "tokens")

    @staticmethod
    def add_card_rulings_table_schema(schema: Dict[str, Any]) -> None:
        schema["card_rulings"]["text"]["type"] = "VARCHAR"
        schema["card_rulings"]["date"]["type"] = "DATE"

    @staticmethod
    def add_card_legalities_table_schema(schema: Dict[str, Any]) -> None:
        schema["card_legalities"]["format"]["type"] = "VARCHAR"
        schema["card_legalities"]["status"]["type"] = "VARCHAR"

    @staticmethod
    def add_card_foreign_data_table_schema(schema: Dict[str, Any]) -> None:
        schema["card_foreign_data"]["flavor_text"]["type"] = "VARCHAR"
        schema["card_foreign_data"]["language"]["type"] = "VARCHAR"
        schema["card_foreign_data"]["multiverseid"]["type"] = "INTEGER"
        schema["card_foreign_data"]["name"]["type"] = "VARCHAR"
        schema["card_foreign_data"]["text"]["type"] = "VARCHAR"
        schema["card_foreign_data"]["type"]["type"] = "VARCHAR"

    @staticmethod
    def add_set_translation_table_schema(schema: Dict[str, Any]) -> None:
        schema["set_translations"]["language"]["type"] = "VARCHAR"
        schema["set_translations"]["translation"]["type"] = "VARCHAR"

    @staticmethod
    def convert_dict_to_sql_query(schema: Dict[str, Any]) -> str:
        q = ""
        for table_name, table_data in schema.items():
            q += f"CREATE TABLE `{table_name}` (\n"
            q += "\tid INTEGER PRIMARY KEY AUTO_INCREMENT,\n"
            for attribute in sorted(table_data.keys()):
                q += f"\t{attribute} {table_data[attribute]['type']},\n"
            q = q[:-2] + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n"

        return q[:-2]

    @staticmethod
    def get_sql_type(mixed: Any) -> Optional[str]:
        """
        Return a string with the type of the parameter mixed

        The type depends on the SQL engine in some cases
        """
        if isinstance(mixed, (str, list, dict)):
            return "VARCHAR"
        elif isinstance(mixed, bool):
            return "BOOLEAN"
        elif isinstance(mixed, float):
            return "FLOAT"
        elif isinstance(mixed, int):
            return "INTEGER"
        return None

    def generate_database_insert_statements(self) -> Iterator[str]:
        for statement in self.generate_insert_statement("sets", self.get_next_set()):
            yield statement

        for statement in self.generate_insert_statement(
            "cards", self.get_next_card_like("cards")
        ):
            yield statement

        for statement in self.generate_insert_statement(
            "tokens", self.get_next_card_like("tokens")
        ):
            yield statement

    def generate_insert_statement(
        self, table_name: str, generator: Iterator[Any]
    ) -> Iterator[str]:
        obj = next(generator)
        yield self.create_insert_statement_header(table_name, obj)

        while obj:
            insert_body = self.create_insert_statement_body(obj)
            next_obj = next(generator, None)

            eol_symbol = "," if next_obj else ";"
            yield insert_body + eol_symbol + "\n"

            obj = next_obj

    @staticmethod
    def create_insert_statement_header(table: str, data: Dict[str, Any]) -> str:
        data_keys = ", ".join(data.keys())
        return f"INSERT INTO {table} ({data_keys}) VALUES\n"

    @staticmethod
    def create_insert_statement_body(data: Dict[str, Any]) -> str:
        pre_processed_values = []
        for value in data.values():
            if isinstance(value, list):
                pre_processed_values.append(
                    '"'
                    + pymysql.converters.escape_string(", ".join(map(str, value)))
                    + '"'
                )
            else:
                pre_processed_values.append(
                    pymysql.converters.escape_string(str(value))
                )

        return "(" + ", ".join(pre_processed_values) + ")"

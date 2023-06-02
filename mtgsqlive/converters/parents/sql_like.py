import abc
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional

import humps

from .abstract import AbstractConverter

nested_dict: Any = lambda: defaultdict(nested_dict)


class SqlLikeConverter(AbstractConverter, abc.ABC):
    @abc.abstractmethod
    def create_insert_statement_body(self, data: Dict[str, Any]) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def write_statements_to_file(self, data_generator: Iterator[str]) -> None:
        raise NotImplementedError()

    def generate_database_insert_statements(self) -> Iterator[str]:
        for generator in self.__get_local_generators():
            for statement in generator:
                yield statement

    def __get_local_generators(self) -> List[Iterator[str]]:
        return [
            self.__generate_insert_statement("meta", self.get_metadata()),
            self.__generate_insert_statement("sets", self.get_next_set()),
            self.__generate_insert_statement("cards", self.get_next_card_like("cards")),
            self.__generate_insert_statement(
                "tokens", self.get_next_card_like("tokens")
            ),
            self.__generate_insert_statement(
                "card_identifiers", self.get_next_card_identifier("cards")
            ),
            self.__generate_insert_statement(
                "card_legalities", self.get_next_card_legalities("cards")
            ),
            self.__generate_insert_statement(
                "card_rulings", self.get_next_card_ruling_entry("cards")
            ),
            self.__generate_insert_statement(
                "card_foreign_data", self.get_next_card_foreign_data_entry("cards")
            ),
            self.__generate_insert_statement(
                "card_purchase_urls", self.get_next_card_purchase_url_entry("cards")
            ),
            self.__generate_insert_statement(
                "set_translations",
                self.get_next_set_field_with_normalization("translations"),
            ),
        ]

    def __generate_insert_statement(
        self, table_name: str, data_generator: Iterator[Dict[str, Any]]
    ) -> Iterator[str]:
        for obj in data_generator:
            data_keys = ", ".join(map(humps.decamelize, obj.keys()))
            safe_values = self.create_insert_statement_body(obj)
            yield f"INSERT INTO {table_name} ({data_keys}) VALUES ({safe_values});\n"

    def _generate_sql_schema_dict(self) -> Dict[str, Any]:
        schema = nested_dict()

        self._add_meta_table_schema(schema)
        self._add_set_table_schema(schema)
        self._add_card_table_schema(schema)
        self._add_token_table_schema(schema)
        self._add_card_identifiers_table_schema(schema)
        self._add_card_legalities_table_schema(schema)
        self._add_card_rulings_table_schema(schema)
        self._add_card_foreign_data_table_schema(schema)
        self._add_card_purchase_urls_table_schema(schema)
        self._add_set_translation_table_schema(schema)

        return dict(schema)

    @staticmethod
    def _add_meta_table_schema(schema: Dict[str, Any]) -> None:
        schema["meta"]["date"]["type"] = "DATE"
        schema["meta"]["version"]["type"] = "TEXT"

    def _add_set_table_schema(self, schema: Dict[str, Any]) -> None:
        for set_data in self.mtgjson_data["data"].values():
            for set_attribute, set_attribute_data in set_data.items():
                if set_attribute in self.skipable_set_keys:
                    continue

                set_attribute = humps.decamelize(set_attribute)
                schema["sets"][set_attribute]["type"] = self._get_sql_type(
                    set_attribute_data
                )

        schema["sets"]["code"]["type"] = "VARCHAR(8) UNIQUE NOT NULL"

    def _get_card_like_schema(self, schema: Dict[str, Any], key_name: str) -> None:
        schema[key_name]["uuid"]["type"] = "VARCHAR(36) NOT NULL"
        for set_data in self.mtgjson_data["data"].values():
            for mtgjson_card in set_data.get(key_name):
                for card_attribute, card_attribute_data in mtgjson_card.items():
                    if card_attribute in self.skipable_card_keys:
                        continue

                    card_attribute = humps.decamelize(card_attribute)
                    schema[key_name][card_attribute]["type"] = self._get_sql_type(
                        card_attribute_data
                    )

    def _add_card_table_schema(self, schema: Dict[str, Any]) -> None:
        self._get_card_like_schema(schema, "cards")

    def _add_token_table_schema(self, schema: Dict[str, Any]) -> None:
        self._get_card_like_schema(schema, "tokens")

    @staticmethod
    def _add_card_rulings_table_schema(schema: Dict[str, Any]) -> None:
        schema["card_rulings"]["text"]["type"] = "TEXT"
        schema["card_rulings"]["date"]["type"] = "DATE"
        schema["card_rulings"]["uuid"]["type"] = "VARCHAR(36) NOT NULL"

    def __add_card_field_with_normalization(
        self,
        table_name: str,
        schema: Dict[str, Any],
        card_field: str,
        iterate_subfield: bool = False,
    ) -> None:
        schema[table_name]["uuid"]["type"] = "VARCHAR(36) NOT NULL"
        for set_data in self.mtgjson_data["data"].values():
            for mtgjson_card in set_data.get("cards"):
                for card_field_sub_entry in mtgjson_card.get(card_field, []):
                    if iterate_subfield:
                        for key in card_field_sub_entry:
                            schema[table_name][humps.decamelize(key)]["type"] = "TEXT"
                    else:
                        schema[table_name][humps.decamelize(card_field_sub_entry)][
                            "type"
                        ] = "TEXT"

    def _add_card_identifiers_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "card_identifiers", schema, "identifiers"
        )

    def _add_card_legalities_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "card_legalities", schema, "legalities"
        )

    def _add_card_foreign_data_table_schema(self, schema: Dict[str, Any]) -> None:
        self.__add_card_field_with_normalization(
            "card_foreign_data", schema, "foreignData", True
        )
        schema["card_foreign_data"]["multiverse_id"]["type"] = "INTEGER"

    def _add_card_purchase_urls_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "card_purchase_urls", schema, "purchaseUrls"
        )

    @staticmethod
    def _add_set_translation_table_schema(schema: Dict[str, Any]) -> None:
        schema["set_translations"]["language"]["type"] = "TEXT"
        schema["set_translations"]["translation"]["type"] = "TEXT"
        schema["set_translations"]["uuid"]["type"] = "VARCHAR(36) NOT NULL"

    @staticmethod
    def _convert_schema_dict_to_query(
        schema: Dict[str, Any],
        engine: str,
        primary_key_op: str,
    ) -> str:
        q = ""
        for table_name, table_data in schema.items():
            q += f"CREATE TABLE {table_name} (\n"
            q += f"\tid {primary_key_op},\n"
            for attribute in sorted(table_data.keys()):
                q += f"\t{attribute} {table_data[attribute]['type']},\n"
            q = f"{q[:-2]}\n){engine};\n\n"

        return q[:-2]

    @staticmethod
    def _get_sql_type(mixed: Any) -> Optional[str]:
        if isinstance(mixed, (str, list, dict)):
            return "TEXT"
        if isinstance(mixed, bool):
            return "BOOLEAN"
        if isinstance(mixed, float):
            return "FLOAT"
        if isinstance(mixed, int):
            return "INTEGER"
        return None

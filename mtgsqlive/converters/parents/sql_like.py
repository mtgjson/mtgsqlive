import abc
import datetime
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional

from ...enums import MtgjsonDataType
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
        if self.data_type == MtgjsonDataType.MTGJSON_CARDS:
            generators = self.__get_mtgjson_card_generators()
        elif self.data_type == MtgjsonDataType.MTGJSON_CARD_PRICES:
            generators = self.__get_mtgjson_card_prices_generators()
        else:
            raise ValueError()

        for generator in generators:
            for statement in generator:
                yield statement

    def __get_mtgjson_card_generators(self) -> List[Iterator[str]]:
        return [
            self.__generate_insert_statement("meta", self.get_metadata()),
            self.__generate_insert_statement("sets", self.get_next_set()),
            self.__generate_insert_statement("cards", self.get_next_card_like("cards")),
            self.__generate_insert_statement(
                "tokens", self.get_next_card_like("tokens")
            ),
            self.__generate_insert_statement(
                "cardIdentifiers", self.get_next_card_identifier("cards")
            ),
            self.__generate_insert_statement(
                "cardLegalities", self.get_next_card_legalities("cards")
            ),
            self.__generate_insert_statement(
                "cardRulings", self.get_next_card_ruling_entry("cards")
            ),
            self.__generate_insert_statement(
                "cardForeignData", self.get_next_card_foreign_data_entry("cards")
            ),
            self.__generate_insert_statement(
                "cardPurchaseUrls", self.get_next_card_purchase_url_entry("cards")
            ),
            self.__generate_insert_statement(
                "tokenIdentifiers", self.get_next_card_identifier("tokens")
            ),
            self.__generate_insert_statement(
                "setTranslations",
                self.get_next_set_field_with_normalization("translations"),
            ),
            self.__generate_insert_statement(
                "setBoosterContents", self.get_next_booster_contents_entry()
            ),
            self.__generate_insert_statement(
                "setBoosterContentWeights", self.get_next_booster_weights_entry()
            ),
            self.__generate_insert_statement(
                "setBoosterSheets", self.get_next_booster_sheets_entry()
            ),
            self.__generate_insert_statement(
                "setBoosterSheetCards", self.get_next_booster_sheet_cards_entry()
            ),
        ]

    def __get_mtgjson_card_prices_generators(self) -> List[Iterator[str]]:
        return [
            self.__generate_batch_insert_statement(
                "cardPrices",
                self.get_next_card_price(
                    datetime.date.today() - datetime.timedelta(days=14)
                ),
            )
        ]

    def __generate_insert_statement(
        self, table_name: str, data_generator: Iterator[Dict[str, Any]]
    ) -> Iterator[str]:
        for obj in data_generator:
            data_keys = ", ".join(obj.keys())
            safe_values = self.create_insert_statement_body(obj)
            yield f"INSERT INTO {table_name} ({data_keys}) VALUES ({safe_values});\n"

    def __generate_batch_insert_statement(
        self, table_name: str, data_generator: Iterator[Dict[str, Any]]
    ) -> Iterator[str]:
        insert_values = []
        data_keys = ""
        for obj in data_generator:
            data_keys = ", ".join(obj.keys())
            safe_values = f"({self.create_insert_statement_body(obj)})"
            insert_values.append(safe_values)

            if len(insert_values) >= 2_000:
                yield_values = ",\n".join(insert_values)
                insert_values = []
                yield f"INSERT INTO {table_name} ({data_keys}) VALUES\n{yield_values};\n"

        yield_values = ",\n".join(insert_values)
        yield f"INSERT INTO {table_name} ({data_keys}) VALUES\n{yield_values};\n"

    def _generate_sql_schema_dict(self) -> Dict[str, Any]:
        schema = nested_dict()

        if self.data_type == MtgjsonDataType.MTGJSON_CARDS:
            self._add_meta_table_schema(schema)
            self._add_set_table_schema(schema)
            self._add_card_table_schema(schema)
            self._add_token_table_schema(schema)
            self._add_card_identifiers_table_schema(schema)
            self._add_card_legalities_table_schema(schema)
            self._add_card_rulings_table_schema(schema)
            self._add_card_foreign_data_table_schema(schema)
            self._add_card_purchase_urls_table_schema(schema)
            self._add_token_identifiers_table_schema(schema)
            self._add_set_translation_table_schema(schema)
            self._add_set_booster_contents_schema(schema)
            self._add_set_booster_content_weights_schema(schema)
            self._add_set_booster_sheets_schema(schema)
            self._add_set_booster_sheet_cards_schema(schema)
        elif self.data_type == MtgjsonDataType.MTGJSON_CARD_PRICES:
            self._add_all_prices_schema(schema)

        return dict(schema)

    @staticmethod
    def _add_meta_table_schema(schema: Dict[str, Any]) -> None:
        schema["meta"]["date"]["type"] = "DATE"
        schema["meta"]["version"]["type"] = "TEXT"

    def _add_set_table_schema(self, schema: Dict[str, Any]) -> None:
        for set_data in self.mtgjson_data["data"].values():
            for set_attribute, set_attribute_data in set_data.items():
                if set_attribute in self.set_keys_to_skip:
                    continue

                schema["sets"][set_attribute]["type"] = self._get_sql_type(
                    set_attribute_data
                )

        schema["sets"]["code"]["type"] = "VARCHAR(8) UNIQUE NOT NULL"

    def _get_card_like_schema(self, schema: Dict[str, Any], key_name: str) -> None:
        for set_data in self.mtgjson_data["data"].values():
            for mtgjson_card in set_data.get(key_name):
                for card_attribute, card_attribute_data in mtgjson_card.items():
                    if card_attribute in self.card_keys_to_skip:
                        continue

                    schema[key_name][card_attribute]["type"] = self._get_sql_type(
                        card_attribute_data
                    )
        schema[key_name]["uuid"]["type"] = "VARCHAR(36) NOT NULL"

    def _add_card_table_schema(self, schema: Dict[str, Any]) -> None:
        self._get_card_like_schema(schema, "cards")

    def _add_token_table_schema(self, schema: Dict[str, Any]) -> None:
        self._get_card_like_schema(schema, "tokens")

    @staticmethod
    def _add_card_rulings_table_schema(schema: Dict[str, Any]) -> None:
        schema["cardRulings"]["text"]["type"] = "TEXT"
        schema["cardRulings"]["date"]["type"] = "DATE"
        schema["cardRulings"]["uuid"]["type"] = "VARCHAR(36) NOT NULL"

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
                            schema[table_name][key]["type"] = "TEXT"
                    else:
                        schema[table_name][card_field_sub_entry]["type"] = "TEXT"

    def _add_card_identifiers_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "cardIdentifiers", schema, "identifiers"
        )

    def _add_card_legalities_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "cardLegalities", schema, "legalities"
        )

    def _add_card_foreign_data_table_schema(self, schema: Dict[str, Any]) -> None:
        self.__add_card_field_with_normalization(
            "cardForeignData", schema, "foreignData", True
        )
        schema["cardForeignData"]["multiverseId"]["type"] = "INTEGER"

    def _add_card_purchase_urls_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "cardPurchaseUrls", schema, "purchaseUrls"
        )

    def _add_token_identifiers_table_schema(self, schema: Dict[str, Any]) -> None:
        return self.__add_card_field_with_normalization(
            "tokenIdentifiers", schema, "identifiers"
        )

    @staticmethod
    def _add_set_translation_table_schema(schema: Dict[str, Any]) -> None:
        schema["setTranslations"]["language"]["type"] = "TEXT"
        schema["setTranslations"]["translation"]["type"] = "TEXT"
        schema["setTranslations"]["uuid"]["type"] = "VARCHAR(36) NOT NULL"

    @staticmethod
    def _add_all_prices_schema(schema: Dict[str, Any]) -> None:
        schema["cardPrices"]["gameAvailability"]["type"] = "VARCHAR(15)"
        schema["cardPrices"]["priceProvider"]["type"] = "VARCHAR(20)"
        schema["cardPrices"]["providerListing"]["type"] = "VARCHAR(15)"
        schema["cardPrices"]["cardFinish"]["type"] = "VARCHAR(15)"
        schema["cardPrices"]["date"]["type"] = "DATE"
        schema["cardPrices"]["price"]["type"] = "FLOAT"
        schema["cardPrices"]["currency"]["type"] = "VARCHAR(10)"
        schema["cardPrices"]["uuid"]["type"] = "VARCHAR(36) NOT NULL"

    @staticmethod
    def _add_set_booster_contents_schema(schema: Dict[str, Any]) -> None:
        schema["setBoosterContents"]["setCode"]["type"] = "VARCHAR(20)"
        schema["setBoosterContents"]["boosterName"]["type"] = "VARCHAR(255)"
        schema["setBoosterContents"]["boosterIndex"]["type"] = "INTEGER"
        schema["setBoosterContents"]["sheetName"]["type"] = "VARCHAR(255)"
        schema["setBoosterContents"]["sheetPicks"]["type"] = "INTEGER"

    @staticmethod
    def _add_set_booster_content_weights_schema(schema: Dict[str, Any]) -> None:
        schema["setBoosterContentWeights"]["setCode"]["type"] = "VARCHAR(20)"
        schema["setBoosterContentWeights"]["boosterName"]["type"] = "VARCHAR(255)"
        schema["setBoosterContentWeights"]["boosterIndex"]["type"] = "INTEGER"
        schema["setBoosterContentWeights"]["boosterWeight"]["type"] = "INTEGER"

    @staticmethod
    def _add_set_booster_sheets_schema(schema: Dict[str, Any]) -> None:
        schema["setBoosterSheets"]["setCode"]["type"] = "VARCHAR(20)"
        schema["setBoosterSheets"]["sheetName"]["type"] = "VARCHAR(255)"
        schema["setBoosterSheets"]["sheetIsFoil"]["type"] = "BOOLEAN"
        schema["setBoosterSheets"]["sheetHasBalanceColors"]["type"] = "BOOLEAN"

    @staticmethod
    def _add_set_booster_sheet_cards_schema(schema: Dict[str, Any]) -> None:
        schema["setBoosterSheetCards"]["setCode"]["type"] = "VARCHAR(20)"
        schema["setBoosterSheetCards"]["sheetName"]["type"] = "VARCHAR(255)"
        schema["setBoosterSheetCards"]["cardUuid"]["type"] = "VARCHAR(36) NOT NULL"
        schema["setBoosterSheetCards"]["cardWeight"]["type"] = "BIGINT"

    @staticmethod
    def _convert_schema_dict_to_query(
        schema: Dict[str, Any],
        engine: str,
        primary_key_op: Optional[str],
    ) -> str:
        q = ""
        for table_name, table_data in schema.items():
            q += f"CREATE TABLE {table_name} (\n"
            if primary_key_op:
                q += f"\tid {primary_key_op},\n"
            for attribute in sorted(table_data.keys()):
                q += f"\t{attribute} {table_data[attribute]['type']},\n"
            q = f"{q[:-2]}\n){engine};\n\n"

            if "uuid" in table_data.keys():
                q = f"{q[:-2]}\nCREATE INDEX {table_name}_uuid ON {table_name}(uuid);\n\n"

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

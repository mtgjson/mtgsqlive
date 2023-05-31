import abc
import copy
import pathlib
from sqlite3 import Connection
from typing import Any, Dict, Iterator, Optional, TextIO

import humps


class OutputObject:
    fp: TextIO | Connection
    root_dir: pathlib.Path

    def __init__(self, root_dir: pathlib.Path):
        self.root_dir = root_dir


class AbstractConverter(abc.ABC):
    mtgjson_data: Dict[str, Any]
    output_obj: OutputObject

    skipable_set_keys = ["booster", "cards", "sealedProduct", "tokens", "translations"]
    skipable_card_keys = [
        "convertedManaCost",
        "foreignData",
        "identifiers",
        "legalities",
        "purchaseUrls",
        "rulings",
    ]

    def __init__(self, mtgjson_data: Dict[str, Any], output_dir: str) -> None:
        self.mtgjson_data = mtgjson_data
        self.output_obj = OutputObject(pathlib.Path(output_dir).expanduser())

    @abc.abstractmethod
    def convert(self) -> None:
        raise NotImplementedError()

    def get_metadata(self) -> Iterator[Dict[str, Any]]:
        yield self.mtgjson_data.get("meta", {})

    def get_version(self) -> Optional[str]:
        return str(self.mtgjson_data["meta"]["version"])

    def get_next_set(self) -> Iterator[Dict[str, Any]]:
        for set_data in self.mtgjson_data["data"].values():
            set_data = copy.deepcopy(set_data)
            for set_attribute in self.skipable_set_keys:
                if set_attribute in set_data:
                    del set_data[set_attribute]
            yield set_data

    def get_next_set_field_with_normalization(
        self, set_attribute: str
    ) -> Iterator[Dict[str, Any]]:
        for set_code, set_data in self.mtgjson_data["data"].items():
            if not set_data.get(set_attribute):
                continue

            set_data[set_attribute]["set_code"] = set_code
            yield {
                humps.camelize(key): value
                for key, value in set_data[set_attribute].items()
            }

    def get_next_card_like(self, set_attribute: str) -> Iterator[Dict[str, Any]]:
        for set_data in self.mtgjson_data["data"].values():
            set_data = copy.deepcopy(set_data)
            for card in set_data.get(set_attribute):
                for card_attribute in self.skipable_card_keys:
                    if card_attribute in card:
                        del card[card_attribute]
                yield card

    def get_next_card_identifier(self, set_attribute: str) -> Iterator[Dict[str, Any]]:
        return self.get_next_card_field_with_normalization(set_attribute, "identifiers")

    def get_next_card_legalities(self, set_attribute: str) -> Iterator[Dict[str, Any]]:
        return self.get_next_card_field_with_normalization(set_attribute, "legalities")

    def get_next_card_ruling_entry(
        self, set_attribute: str
    ) -> Iterator[Dict[str, Any]]:
        return self.get_next_card_field_with_normalization(set_attribute, "rulings")

    def get_next_card_foreign_data_entry(
        self, set_attribute: str
    ) -> Iterator[Dict[str, Any]]:
        return self.get_next_card_field_with_normalization(set_attribute, "foreignData")

    def get_next_card_purchase_url_entry(
        self, set_attribute: str
    ) -> Iterator[Dict[str, Any]]:
        return self.get_next_card_field_with_normalization(
            set_attribute, "purchaseUrls"
        )

    def get_next_card_field_with_normalization(
        self, set_attribute: str, secondary_attribute: str
    ) -> Iterator[Dict[str, Any]]:
        for set_data in self.mtgjson_data["data"].values():
            set_data = copy.deepcopy(set_data)
            for card in set_data.get(set_attribute):
                if isinstance(card[secondary_attribute], list):
                    for sub_entity in card[secondary_attribute]:
                        yield self.__camelize_and_normalize_card(sub_entity, card)
                else:
                    yield self.__camelize_and_normalize_card(
                        card[secondary_attribute], card
                    )

    @staticmethod
    def __camelize_and_normalize_card(
        entity: Dict[str, Any], card: Dict[str, Any]
    ) -> Dict[str, Any]:
        entity["uuid"] = card.get("uuid")
        return {humps.camelize(key): value for key, value in entity.items()}

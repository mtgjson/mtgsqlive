import abc
import copy
import pathlib
from typing import Any, Dict, TextIO, Optional

import humps


class OutputObject:
    fp: Optional[TextIO]
    dir: pathlib.Path

    def __init__(self, fp, dir):
        self.fp = fp
        self.dir = dir


class AbstractConverter(abc.ABC):
    mtgjson_data: Dict[str, Any]
    output_obj: OutputObject

    skipable_set_keys = ["cards", "tokens", "booster", "sealedProduct", "translations"]
    skipable_card_keys = [
        "convertedManaCost",
        "legalities",
        "foreignData",
        "rulings",
        "identifiers",
    ]

    def __init__(self, mtgjson_data: Dict[str, Any], output_path: str):
        self.mtgjson_data = mtgjson_data
        self.output_obj = OutputObject(None, pathlib.Path(output_path).expanduser())

    @abc.abstractmethod
    def convert(self):
        pass

    def get_metadata(self):
        yield self.mtgjson_data.get("meta", {})

    def get_version(self) -> Optional[str]:
        return self.mtgjson_data.get("meta", {}).get("version")

    def get_next_set(self):
        for set_code, set_data in self.mtgjson_data.get("data").items():
            set_data = copy.deepcopy(set_data)
            for set_attribute in self.skipable_set_keys:
                if set_attribute in set_data:
                    del set_data[set_attribute]
            yield set_data

    def get_next_card_like(self, set_attribute):
        for set_code, set_data in self.mtgjson_data.get("data").items():
            set_data = copy.deepcopy(set_data)
            for card in set_data.get(set_attribute):
                for card_attribute in self.skipable_card_keys:
                    if card_attribute in card:
                        del card[card_attribute]
                yield card

    def get_next_card_field_with_normalization(
        self, set_attribute, secondary_attribute
    ):
        for set_code, set_data in self.mtgjson_data.get("data").items():
            set_data = copy.deepcopy(set_data)
            for card in set_data.get(set_attribute):
                card[secondary_attribute]["uuid"] = card.get("uuid")

                yield {
                    humps.camelize(key): value
                    for key, value in card[secondary_attribute].items()
                }

    def get_next_card_identifier(self, set_attribute):
        return self.get_next_card_field_with_normalization(set_attribute, "identifiers")

    def get_next_card_legalities(self, set_attribute):
        return self.get_next_card_field_with_normalization(set_attribute, "legalities")

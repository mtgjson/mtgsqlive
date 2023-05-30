import abc
import copy
import pathlib
from typing import Any, Dict, TextIO, Optional


class OutputObject:
    fp: Optional[TextIO]
    dir: pathlib.Path

    def __init__(self, fp, dir):
        self.fp = fp
        self.dir = dir


class AbstractConverter(abc.ABC):
    mtgjson_data: Dict[str, Any]
    output_obj: OutputObject

    skipable_set_keys = ["cards", "tokens", "booster", "sealedProduct"]
    skipable_card_keys = ["legalities", "rulings", "foreignData", "purchaseUrls"]

    def __init__(self, mtgjson_data: Dict[str, Any], output_path: str):
        self.mtgjson_data = mtgjson_data
        self.output_obj = OutputObject(None, pathlib.Path(output_path).expanduser())

    @abc.abstractmethod
    def convert(self):
        pass

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

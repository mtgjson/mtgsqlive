import abc
import copy
import datetime
import pathlib
from sqlite3 import Connection
from typing import Any, Dict, Iterator, Optional, TextIO

from ...enums import MtgjsonDataType


class OutputObject:
    fp: TextIO | Connection
    root_dir: pathlib.Path

    def __init__(self, root_dir: pathlib.Path):
        self.root_dir = root_dir


class AbstractConverter(abc.ABC):
    mtgjson_data: Dict[str, Any]
    output_obj: OutputObject
    data_type: MtgjsonDataType

    set_keys_to_skip = [
        "booster",  # WIP for own table
        "cards",  # Broken out into cards
        "sealedProduct",  # WIP for own table
        "tokens",  # Broken out into tokens
        "translations",  # Broken out into setTranslations
    ]
    card_keys_to_skip = [
        "convertedManaCost",  # Redundant with manaValue
        "foreignData",  # Broken out into cardForeignData
        "identifiers",  # Broken out into cardIdentifiers & tokenIdentifiers
        "legalities",  # Broken out into cardLegalities
        "purchaseUrls",  # Broken out into cardPurchaseUrls
        "rulings",  # Broken out into cardRulings
    ]

    def __init__(
        self, mtgjson_data: Dict[str, Any], output_dir: str, data_type: MtgjsonDataType
    ) -> None:
        self.mtgjson_data = mtgjson_data
        self.output_obj = OutputObject(pathlib.Path(output_dir).expanduser())
        self.data_type = data_type

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
            for set_attribute in self.set_keys_to_skip:
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
            yield set_data[set_attribute]

    def get_next_card_like(self, set_attribute: str) -> Iterator[Dict[str, Any]]:
        for set_data in self.mtgjson_data["data"].values():
            set_data = copy.deepcopy(set_data)
            for card in set_data.get(set_attribute):
                for card_attribute in self.card_keys_to_skip:
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
                if secondary_attribute not in card:
                    continue

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
        return entity

    def get_next_card_price(
        self,
        oldest_date: datetime.date,
    ) -> Iterator[Dict[str, str]]:
        oldest_date_str = str(oldest_date)

        for card_uuid, card_uuid_data in self.mtgjson_data["data"].items():
            for game_availability, game_availability_data in card_uuid_data.items():
                for (
                    price_provider,
                    price_provider_data,
                ) in game_availability_data.items():
                    currency = price_provider_data["currency"]
                    for (
                        provider_listing,
                        provider_listing_data,
                    ) in price_provider_data.items():
                        if provider_listing == "currency":
                            continue

                        for (
                            card_finish,
                            card_finish_data,
                        ) in provider_listing_data.items():
                            for price_date, price_amount in card_finish_data.items():
                                if price_date < oldest_date_str:
                                    continue
                                yield {
                                    "uuid": card_uuid,
                                    "gameAvailability": game_availability,
                                    "priceProvider": price_provider,
                                    "providerListing": provider_listing,
                                    "cardFinish": card_finish,
                                    "date": price_date,
                                    "price": price_amount,
                                    "currency": currency,
                                }

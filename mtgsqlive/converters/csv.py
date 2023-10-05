from typing import Any, Dict

from ..enums import MtgjsonDataType
from .parents import SqliteBasedConverter


class CsvConverter(SqliteBasedConverter):
    def __init__(
        self, mtgjson_data: Dict[str, Any], output_dir: str, data_type: MtgjsonDataType
    ) -> None:
        super().__init__(mtgjson_data, output_dir, data_type)
        self.output_obj.root_dir.joinpath("csv").mkdir(parents=True, exist_ok=True)

    def convert(self) -> None:
        for table_name in self.get_table_names():
            pd_table = self.get_table_dataframe(table_name)

            pd_table.to_csv(
                str(
                    self.output_obj.root_dir.joinpath("csv").joinpath(
                        f"{table_name}.csv"
                    )
                ),
                encoding="utf-8",
                index=False,
            )

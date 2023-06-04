from typing import Any, Dict

import pandas as pd

from ..enums.data_type import MtgjsonDataType
from .parents import SqliteBasedConverter


class CsvConverter(SqliteBasedConverter):
    def __init__(
        self, mtgjson_data: Dict[str, Any], output_dir: str, data_type: MtgjsonDataType
    ) -> None:
        super().__init__(mtgjson_data, output_dir, data_type)
        self.output_obj.root_dir.joinpath("csv").mkdir(parents=True, exist_ok=True)

    def convert(self) -> None:
        for table_name in self.get_next_table_name():
            pd_table = pd.read_sql_query(
                f"SELECT * FROM `{table_name}`", self.sqlite_db
            )

            pd_table.to_csv(
                str(
                    self.output_obj.root_dir.joinpath("csv").joinpath(
                        f"{table_name}.csv"
                    )
                ),
                encoding="utf-8",
            )

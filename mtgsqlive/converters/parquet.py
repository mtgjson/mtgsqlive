from typing import Any, Dict

import pandas as pd
import pyarrow
import pyarrow.parquet

from ..enums.data_type import MtgjsonDataType
from .parents import SqliteBasedConverter


class ParquetConverter(SqliteBasedConverter):
    def __init__(
        self, mtgjson_data: Dict[str, Any], output_dir: str, data_type: MtgjsonDataType
    ) -> None:
        super().__init__(mtgjson_data, output_dir, data_type)
        self.output_obj.root_dir.joinpath("parquet").mkdir(parents=True, exist_ok=True)

    def convert(self) -> None:
        for table_name in self.get_next_table_name():
            pd_table = pd.read_sql_query(
                f"SELECT * FROM `{table_name}`", self.sqlite_db
            )

            parquet_table = pyarrow.Table.from_pandas(
                pd_table, preserve_index=False
            ).replace_schema_metadata(self.get_metadata())

            pyarrow.parquet.write_table(
                parquet_table,
                str(
                    self.output_obj.root_dir.joinpath("parquet").joinpath(
                        f"{table_name}.parquet"
                    )
                ),
            )

from typing import Any, Dict

import pyarrow
import pyarrow.parquet

from ..enums import MtgjsonDataType
from .parents import SqliteBasedConverter


class ParquetConverter(SqliteBasedConverter):
    def __init__(
        self, mtgjson_data: Dict[str, Any], output_dir: str, data_type: MtgjsonDataType
    ) -> None:
        super().__init__(mtgjson_data, output_dir, data_type)
        self.output_obj.root_dir.joinpath("parquet").mkdir(parents=True, exist_ok=True)

    def convert(self) -> None:
        for table_name in self.get_table_names():
            pd_table = self.get_table_dataframe(table_name)

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

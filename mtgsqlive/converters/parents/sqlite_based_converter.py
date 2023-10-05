import abc
from typing import Any, Dict, List

import pandas as pd
import sqlalchemy

from ...enums import MtgjsonDataType
from .abstract import AbstractConverter


class SqliteBasedConverter(AbstractConverter, abc.ABC):
    sqlite_engine: sqlalchemy.Engine

    def __init__(
        self, mtgjson_data: Dict[str, Any], output_dir: str, data_type: MtgjsonDataType
    ) -> None:
        super().__init__(mtgjson_data, output_dir, data_type)

        db_path = self.output_obj.root_dir.joinpath(f"{data_type.value}.sqlite")
        if not db_path.exists():
            raise FileNotFoundError()

        self.sqlite_engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")

    def get_table_names(self) -> List[str]:
        with self.sqlite_engine.connect() as connection:
            result = connection.execute(sqlalchemy.text("""SELECT name FROM sqlite_master WHERE type = 'table';"""))
            table_names = [r.name for r in result]
        return table_names

    def get_table_dataframe(self, table_name: str) -> pd.DataFrame:
        return pd.read_sql_table(table_name, self.sqlite_engine)

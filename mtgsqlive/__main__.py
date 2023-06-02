import argparse
import json
import logging
import pathlib
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict

from mtgsqlive.converters import (
    CsvConverter,
    MysqlConverter,
    ParquetConverter,
    PostgresqlConverter,
    SqliteConverter,
)

TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
LOG_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")
LOGGER = logging.getLogger(__name__)


def init_logger() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                str(
                    LOG_DIR.joinpath(
                        "mtgsqlive_"
                        + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        + ".log"
                    )
                )
            ),
        ],
    )


def get_converters() -> Dict[str, Any]:
    return OrderedDict(
        {
            "mysql": MysqlConverter,
            "postgresql": PostgresqlConverter,
            "sqlite": SqliteConverter,
            "csv": CsvConverter,
            "parquet": ParquetConverter,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input-file",
        type=str,
        required=True,
        help="Path to MTGJSON AllPrintings.json",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default="/tmp/mtgsqlive",
        help="Where to place translated files",
    )

    converter_group = parser.add_argument_group(title="Converters")
    converter_group.add_argument(
        "--all", action="store_true", help="Run all ETL operations"
    )
    converter_group.add_argument(
        "--csv", action="store_true", help="Compile CSV AllPrinting files"
    )
    converter_group.add_argument(
        "--mysql", action="store_true", help="Compile AllPrintings.sql"
    )
    converter_group.add_argument(
        "--parquet", action="store_true", help="Compile Parquet AllPrinting files"
    )
    converter_group.add_argument(
        "--postgresql", action="store_true", help="Compile AllPrintings.psql"
    )
    converter_group.add_argument(
        "--sqlite", action="store_true", help="Compile AllPrintings.sqlite"
    )

    return parser.parse_args()


def main() -> None:
    init_logger()

    args = parse_args()

    mtgjson_input_file = pathlib.Path(args.input_file).expanduser()
    if not mtgjson_input_file.exists():
        LOGGER.error(f"Cannot locate {mtgjson_input_file}, exiting.")
        return

    with mtgjson_input_file.open(encoding="utf-8") as fp:
        mtgjson_input_data = json.load(fp)

    converters_map = get_converters()
    if not args.all:
        for converter_input_param, converter in converters_map.copy().items():
            if not getattr(args, converter_input_param):
                del converters_map[converter_input_param]

    for converter in converters_map.values():
        LOGGER.info(f"Starting conversion via {converter.__name__}")
        converter(mtgjson_input_data, args.output_dir).convert()
        LOGGER.info(f"Finished conversion via {converter.__name__}")


if __name__ == "__main__":
    main()

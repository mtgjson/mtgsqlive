import argparse
import itertools
import json
import logging
import multiprocessing
import pathlib
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List

from mtgsqlive.converters import (
    CsvConverter,
    MysqlConverter,
    ParquetConverter,
    PostgresqlConverter,
    SqliteConverter,
)
from mtgsqlive.enums import MtgjsonDataType

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
            "mysql": (MysqlConverter, 0),
            "postgresql": (PostgresqlConverter, 0),
            "sqlite": (SqliteConverter, 0),
            "csv": (CsvConverter, 1),
            "parquet": (ParquetConverter, 1),
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input-dir",
        type=str,
        required=True,
        help="Path to directory that has MTGJSON compiled files, like AllPrintings.json and AllPrices.json",
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


def get_converter_groups(args: argparse.Namespace) -> List[List[Any]]:
    parallel_converter_groups: List[List[Any]] = [[], []]
    for converter_input_param, (converter, priority) in get_converters().items():
        if getattr(args, converter_input_param) or args.all:
            parallel_converter_groups[priority].append(converter)

    return parallel_converter_groups


def convert_mtgjson_data(
    converter: Any, mtgjson_input_dir: pathlib.Path, output_dir: str
) -> None:
    init_logger()
    for data_type in MtgjsonDataType:
        mtgjson_input_file = mtgjson_input_dir.joinpath(f"{data_type.value}.json")
        if not mtgjson_input_file.exists():
            LOGGER.error(f"Cannot locate {mtgjson_input_file}, skipping.")
            continue

        with mtgjson_input_file.open(encoding="utf-8") as fp:
            mtgjson_input_data = json.load(fp)

        LOGGER.info(f"Converting {data_type.value} via {converter.__name__}")
        converter(mtgjson_input_data, output_dir, data_type).convert()
        LOGGER.info(f"Converted {data_type.value} via {converter.__name__}")


def main() -> None:
    args = parse_args()

    mtgjson_input_dir = pathlib.Path(args.input_dir).expanduser()
    parallel_converter_groups = get_converter_groups(args)
    for converter_group in parallel_converter_groups:
        with multiprocessing.Pool() as pool:
            pool.starmap(
                convert_mtgjson_data,
                zip(
                    converter_group,
                    itertools.repeat(mtgjson_input_dir),
                    itertools.repeat(args.output_dir),
                ),
            )
            pool.close()
            pool.join()


if __name__ == "__main__":
    main()

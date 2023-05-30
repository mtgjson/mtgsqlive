import argparse
import json
import logging
import pathlib

from mtgsqlive.converters import (
    CsvConverter,
    MysqlConverter,
    ParquetConverter,
    PostgresqlConverter,
    SqliteConverter,
)

LOGGER = logging.getLogger(__name__)


def get_converters():
    return {
        "csv": CsvConverter,
        "mysql": MysqlConverter,
        "parquet": ParquetConverter,
        "postgresql": PostgresqlConverter,
        "sqlite": SqliteConverter,
    }


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input-file",
        type=str,
        required=True,
        help="MTGJSON v5 AllPrintings.json path",
    )
    parser.add_argument("-o", "--output-dir", type=str, default="~/Desktop/")

    converter_group = parser.add_argument_group(title="Converter Types")
    converter_group.add_argument(
        "--csv", action="store_true", help="Compile AllPrintings.csv"
    )
    converter_group.add_argument(
        "--mysql", action="store_true", help="Compile AllPrintings.sql"
    )
    converter_group.add_argument(
        "--parquet", action="store_true", help="Compile AllPrintings.parquet"
    )
    converter_group.add_argument(
        "--postgresql", action="store_true", help="Compile AllPrintings.psql"
    )
    converter_group.add_argument(
        "--sqlite", action="store_true", help="Compile AllPrintings.sqlite"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    mtgjson_input_file = pathlib.Path(args.input_file).expanduser()
    if not mtgjson_input_file.exists():
        LOGGER.error(f"Cannot locate {mtgjson_input_file}, exiting.")
        return

    with mtgjson_input_file.open() as fp:
        mtgjson_input_data = json.load(fp)

    converters_map = get_converters()
    for converter_input_param, converter in converters_map.copy().items():
        if not getattr(args, converter_input_param):
            del converters_map[converter_input_param]

    for converter in converters_map.values():
        converter(mtgjson_input_data, args.output_dir).convert()


if __name__ == "__main__":
    main()

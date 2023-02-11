"""
Main Executor
"""
import argparse
import logging
import pathlib

import mtgsqlive
from mtgsqlive import sql2csv, json2sql, parquet

if __name__ == "__main__":
    mtgsqlive.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        help="input source (AllPrintings.json)",
        required=True,
        metavar="fileIn",
    )
    parser.add_argument(
        "-o",
        help="output folder (outputs/)",
        default="outputs",
        metavar="fileOut"
    )
    parser.add_argument(
        "--formats",
        help="Formats to output. By default all outputs are produced",
        nargs="+",
        choices=["sqlite", "sql", "csv", "parquet"],
        default=["sqlite", "sql", "csv", "parquet"]
    )
    parser.add_argument(
        "-x",
        help="Check for extra input files",
        action="store_true",
        required=False,
    )
    args = parser.parse_args()

    # Define our I/O paths
    input_file = pathlib.Path(args.i).expanduser()
    output_file = {"path": pathlib.Path(args.o).expanduser().absolute(), "handle": None}
    output_file["path"].mkdir(exist_ok=True, parents=True)

    # CSV requires sqlite
    if "sqlite" in args.formats or "csv" in args.formats:
        logging.info("> Creating AllPrintings.sqlite")
        json2sql.execute(
            input_file,
            {
                "path":  output_file["path"] / "AllPrintings.sqlite",
                "handle": None,
            },
            args.x,
        )
    if "sql" in args.formats:
        logging.info("> Creating AllPrintings.sql")
        json2sql.execute(
            input_file,
            {"path": output_file["path"] / "AllPrintings.sql", "handle": None},
            args.x,
        )
    if "csv" in args.formats:
        logging.info("> Creating AllPrintings CSV components")
        sql2csv.execute(
            output_file["path"] / "AllPrintings.sqlite",
            {"path": output_file["path"].joinpath("csv"), "handle": None},
        )
    if "parquet" in args.formats:
        logging.info("> Creating AllPrintings.parquet")
        parquet.execute(input_file, output_file["path"] / "AllPrintings.parquet")

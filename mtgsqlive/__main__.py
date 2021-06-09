"""
Main Executor
"""
import argparse
import logging
import pathlib

import mtgsqlive
from mtgsqlive import json2sql, sql2csv

if __name__ == "__main__":
    mtgsqlive.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input-file",
        help="input source (AllPrintings.json)",
        required=True,
        metavar="fileIn",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        help="output folder (outputs/)",
        default="outputs",
        required=True,
        metavar="fileOut",
    )
    parser.add_argument(
        "-a",
        "--all",
        help="build all types (SQLite, SQL, CSV)",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-x",
        "--extra",
        help="check for extra input files",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-e",
        "--engine",
        help="database engine ('postgres' or 'mysql') for .sql output",
        default="mysql",
        required=False,
        metavar="engine",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="verbose output",
        action="store_true",
        required=False,
    )
    args = parser.parse_args()

    # Define our I/O paths
    input_file = pathlib.Path(args.input_file).expanduser()
    output_file = {"path": pathlib.Path(args.output_file).expanduser().absolute(), "handle": None}

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.all:
        logging.info("> Creating AllPrintings.sqlite")
        json2sql.execute(
            input_file,
            {
                "path": output_file["path"].joinpath("AllPrintings.sqlite"),
                "handle": None,
            },
            args.extra,
        )

        logging.info("> Creating AllPrintings.sql")
        json2sql.execute(
            input_file,
            {"path": output_file["path"].joinpath("AllPrintings.sql"), "handle": None},
            args.extra,
            args.engine,
        )

        logging.info("> Creating AllPrintings CSV components")
        sql2csv.execute(
            output_file["path"].joinpath("AllPrintings.sqlite"),
            {"path": output_file["path"].joinpath("csv"), "handle": None},
        )
    elif str(input_file).endswith(".sqlite"):
        sql2csv.execute(input_file, output_file)
    else:
        json2sql.execute(input_file, output_file, args.extra, args.engine)

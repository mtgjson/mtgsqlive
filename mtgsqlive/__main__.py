"""
Main Executor
"""
import argparse
import logging
import pathlib

import mtgsqlive
from mtgsqlive import sql2csv, json2sql

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
        required=True,
        metavar="fileOut",
    )
    parser.add_argument(
        "--all",
        help="Build all types (SQLite, SQL, CSV)",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-x",
        help="Check for extra input files",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-e",
        help="SQL database engine ('postgres' or 'mysql'). Only used if output file has .sql extension.",
        default="postgres",
        required=False,
    )
    args = parser.parse_args()

    # Define our I/O paths
    input_file = pathlib.Path(args.i).expanduser()
    output_file = {"path": pathlib.Path(args.o).expanduser().absolute(), "handle": None}

    if args.all:
        logging.info("> Creating AllPrintings.sqlite")
        json2sql.execute(
            input_file,
            {
                "path": output_file["path"].joinpath("AllPrintings.sqlite"),
                "handle": None,
            },
            args.x,
        )

        logging.info("> Creating AllPrintings.sql")
        json2sql.execute(
            input_file,
            {"path": output_file["path"].joinpath("AllPrintings.sql"), "handle": None},
            args.x,
            args.e,
        )

        logging.info("> Creating AllPrintings CSV components")
        sql2csv.execute(
            output_file["path"].joinpath("AllPrintings.sqlite"),
            {"path": output_file["path"].joinpath("csv"), "handle": None},
        )
    elif str(input_file).endswith(".sqlite"):
        sql2csv.execute(input_file, output_file)
    else:
        json2sql.execute(input_file, output_file, args.x, args.e)

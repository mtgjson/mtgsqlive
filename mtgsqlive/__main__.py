"""
Main Executor
"""
import argparse
import pathlib

import mtgsqlive
from mtgsqlive import sql2csv, json2sql

if __name__ == "__main__":
    mtgsqlive.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        help="input source (AllPrintings.json, AllSetFiles.zip, Database.sql)",
        required=True,
        metavar="fileIn",
    )
    parser.add_argument(
        "-o",
        help="output file (*.sqlite, *.db, *.sqlite3, *.db3, *.sql, *.csv)",
        required=True,
        metavar="fileOut",
    )
    args = parser.parse_args()

    # Define our I/O paths
    input_file = pathlib.Path(args.i).expanduser()
    output_file = {"path": pathlib.Path(args.o).expanduser(), "handle": None}

    if str(input_file).endswith(".sqlite"):
        sql2csv.execute(input_file, output_file)
    else:
        json2sql.execute(input_file, output_file)

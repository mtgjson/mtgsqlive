"""
Main Executor
"""
import mtgsqlive
from mtgsqlive.json2sql import main

if __name__ == "__main__":
    mtgsqlive.init_logger()
    main()

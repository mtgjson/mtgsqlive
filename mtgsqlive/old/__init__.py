"""
Initializer for MTGSQLite
"""
import logging
import pathlib
import time

TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
LOG_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")


def init_logger() -> None:
    """
    Logger operations
    """
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                str(
                    LOG_DIR.joinpath(
                        "mtgsqlive_" + str(time.strftime("%Y-%m-%d_%H-%M-%S")) + ".log"
                    )
                )
            ),
        ],
    )

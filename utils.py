import os
import sys
import psutil
import logging
from pathlib import Path


def init_set_up() -> None:
    """
    Create `logs` folder if it doesn't exist.
    Setup basic logging.
    Exit if script is already running.
    """
    # ensure the logs folder exists
    Path("logs").mkdir(parents=True, exist_ok=True)

    # config logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename="logs/backup.out",
    )

    # if this script/file is already running exit
    if is_running():
        logging.warning("Attempted to run the script concurrently!")
        logging.warning(60 * "-")
        sys.exit()


def is_running() -> bool:
    """
    Check if this script is already running.
    Return: True if it's running, False otherwise
    """
    for q in psutil.process_iter():
        if (
            q.name().startswith("python")
            and len(q.cmdline()) > 1
            and sys.argv[0] in q.cmdline()[1]
            and os.getpid() != q.pid
        ):
            return True
    return False

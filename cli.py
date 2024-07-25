import json
import logging
import asyncio
import subprocess
from pathlib import Path
from utils import init_set_up
from itertools import zip_longest


# read config file
config = json.loads(Path("config.json").read_text())


# get config vars
DIRECTORIES = config.get("DIRECTORIES")
BUCKET_NAME = config.get("BUCKET_NAME")
STORAGE_CLASS = config.get("STORAGE_CLASS")
PREFIXES = tuple(config.get("PREFIXES"))
SUFFIXES = tuple(config.get("SUFFIXES"))


async def main() -> None:
    """Sync directories with S3 bucket."""

    # perform initial setup
    init_set_up()

    # sync directories with a bucket
    coros = [asyncio.to_thread(update_bucket, d) for d in DIRECTORIES]
    async with asyncio.TaskGroup() as tg:
        for coro in coros:
            tg.create_task(coro)


def update_bucket(directory: str) -> None:
    """Sync local directory with bucket directory."""

    cmd = [
        "aws",
        "s3",
        "sync",
        directory,
        f"s3://{BUCKET_NAME}{directory}",
        "--storage-class",
        STORAGE_CLASS,
        "--delete",
    ]

    for prefix, suffix in zip_longest(PREFIXES, SUFFIXES):
        cmd += ["--exclude", f"'{prefix}*'"] if prefix else []
        cmd += ["--exclude", f"'*{suffix}'"] if suffix else []

    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0 and result.stderr:
        logging.error(result.stderr.decode())


if __name__ == "__main__":
    asyncio.run(main())

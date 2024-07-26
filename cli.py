import logging
import asyncio
import subprocess
from pathlib import Path
from itertools import zip_longest

import config as cfg
from utils import init_set_up, permitted


async def main() -> None:
    """Sync directories with S3 bucket."""

    # perform initial setup
    init_set_up()

    # sync directories with a bucket dirs
    coros = [asyncio.to_thread(update_bucket, d) for d in cfg.DIRECTORIES]
    async with asyncio.TaskGroup() as tg:
        for coro in coros:
            tg.create_task(coro)


def get_excluded_dirs(root_dir: str) -> set[str]:
    """
    Find dir paths that need to be ignored as per the PREFIXES and SUFFIXES.
    Return: set of strings - paths to dirs.
    """
    excluded = set()
    for current_root, dir_names, _ in Path(root_dir).walk():
        # update excluded_dirs
        excluded |= {str(current_root / d) for d in dir_names if not permitted(d)}
        # change dir_names var for the next recursion steps without excluded dirs
        dir_names[:] = [d for d in dir_names if permitted(d)]

    return excluded


def update_bucket(directory: str) -> None:
    """Sync local directory with bucket directory."""

    cmd = [
        "aws",
        "s3",
        "sync",
        directory,
        f"s3://{cfg.BUCKET_NAME}{directory}",
        "--storage-class",
        cfg.STORAGE_CLASS,
        "--delete",
    ]

    # ignore dirs
    for dir_path in get_excluded_dirs(directory):
        cmd += ["--exclude", f"{dir_path}/*"]

    # ignore files
    for prefix, suffix in zip_longest(cfg.PREFIXES, cfg.SUFFIXES):
        cmd += ["--exclude", f"{prefix}*"] if prefix else []
        cmd += ["--exclude", f"*{suffix}"] if suffix else []

    # run aws cli in child process
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0 and result.stderr:
        logging.error(result.stderr.decode())


if __name__ == "__main__":
    asyncio.run(main())

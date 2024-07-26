import json
import boto3
import psutil
import asyncio
import logging
import hashlib
import botocore.config
from pathlib import Path
from utils import init_set_up
from botocore.exceptions import ClientError, BotoCoreError


# read config file
config = json.loads(Path("config.json").read_text())


# get config vars
DIRECTORIES = config.get("DIRECTORIES")
BUCKET_NAME = config.get("BUCKET_NAME")
STORAGE_CLASS = config.get("STORAGE_CLASS")
PREFIXES = tuple(config.get("PREFIXES"))
SUFFIXES = tuple(config.get("SUFFIXES"))
MAX_ACTIVE_TASKS = int(config.get("MAX_POOL_SIZE", 0)) or psutil.cpu_count()


# setup boto3
client_config = botocore.config.Config(max_pool_connections=MAX_ACTIVE_TASKS + 1)
s3 = boto3.resource("s3", config=client_config)  # create an S3 resource
BUCKET = s3.Bucket(BUCKET_NAME)  # instantiate an S3 bucket
CLIENT = s3.meta.client  # instantiate an S3 low-level client (thread safe)


async def main() -> None:
    """Sync directories with s3 bucket."""
    # perform initial setup
    init_set_up()

    # compute local file index
    index = await compute_index()

    # aynchronysly update S3 bucket (delete/update/upload files)
    await update_bucket(index)


async def compute_index() -> set[str]:
    """Compute index on every dict concurrently."""

    # execute coroutines concurrently and await for all results to come
    coros = [asyncio.to_thread(compute_dir_index, Path(d)) for d in DIRECTORIES]
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(coro) for coro in coros]

    # merge all sets into one
    return {key for index in tasks for key in index.result()}


def compute_dir_index(root_dir: Path) -> set[str]:
    """
    Computes a directory's index of files.
    root_dir: absolute path to the root directory
    Return: set with files absolute paths
    """
    index = set()
    # exclusion helper function
    permitted = lambda x: not (x.startswith(PREFIXES) or x.endswith(SUFFIXES))
    # traverse the path recursively
    for current_root, dir_names, file_names in root_dir.walk():
        # copy of dir_names without excluded directories
        dir_names[:] = [d for d in dir_names if permitted(d)]
        # copy of file_names without excluded files
        file_names[:] = [f for f in file_names if permitted(f)]
        # loop through the file names in the current directory
        for file_name in file_names:
            # add the file's absolute path to set
            if (filepath := current_root / file_name).exists():
                index.add(str(filepath))
    return index


async def update_bucket(index: set[str]) -> None:
    """
    Create the needed S3 resources and instances and
    delete/upload files concurrently.
    data: dictionary of deleted, new and modified files
    Return: None
    """

    # get all bucket files
    all_bucket_objects = BUCKET.objects.all()
    bucket_files = set(f"/{f.key}" for f in all_bucket_objects)

    # files can be deleted in batches of max 1000 files per batch
    to_delete = [{"Key": key.lstrip("/")} for key in (bucket_files - index)]
    to_delete = [to_delete[i : i + 1000] for i in range(0, len(to_delete), 1000)]
    coros = [asyncio.to_thread(bulk_delete_s3_objects, lst) for lst in to_delete]

    # which files to uplaod
    to_upload = list(index - bucket_files)
    for obj in all_bucket_objects:
        if (key := f"/{obj.key}") not in index:
            continue

        if not (filepath := Path(key)).exists():
            continue

        if obj.size != filepath.stat().st_size:
            to_upload.append(key)
            continue

        if obj.e_tag.strip('"') != e_tag(filepath):
            to_upload.append(key)

    # sort upload files by size
    to_upload.sort(key=lambda f: Path(f).stat().st_size)
    coros += [asyncio.to_thread(upload_s3_object, key) for key in to_upload]

    # quick anonymous function for getting the number of current active tasks
    active_tasks = lambda: sum(1 for t in asyncio.all_tasks() if not t.done())

    # add tasks to group, the context will automatically await them
    async with asyncio.TaskGroup() as tg:
        for coro in coros:
            while active_tasks() >= MAX_ACTIVE_TASKS:
                await asyncio.sleep(0.25)
            tg.create_task(coro)


def bulk_delete_s3_objects(keys: list[dict[str, str]]) -> None:
    """
    Delete multiple s3 objects with one HTTP request.
    The limit is 1000 objects per request.
    keys: list of dicts - {"Key": str}
    """
    try:
        CLIENT.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": keys})
    except (ClientError, BotoCoreError) as e:
        logging.error(e)


def upload_s3_object(key: str) -> None:
    try:
        # on success response is None
        CLIENT.upload_file(
            Filename=key,
            Bucket=BUCKET_NAME,
            Key=key.lstrip("/"),
            ExtraArgs={"StorageClass": STORAGE_CLASS},
        )
    except (ClientError, BotoCoreError) as e:
        logging.error(e)


def e_tag(filepath: Path) -> str:
    with open(filepath, "rb") as f:
        return hashlib.file_digest(f, "md5").hexdigest()


if __name__ == "__main__":
    asyncio.run(main())

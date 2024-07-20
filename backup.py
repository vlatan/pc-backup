import os
import sys
import json
import time
import boto3
import psutil
import asyncio
import logging
import botocore.config
from botocore.exceptions import ClientError, BotoCoreError


# get config variables
with open("config.json", "r") as jsonfile:
    config = json.load(jsonfile)


DIRECTORIES = config.get("DIRECTORIES")
BUCKET_NAME = config.get("BUCKET_NAME")
STORAGE_CLASS = config.get("STORAGE_CLASS")
PREFIXES = tuple(config.get("PREFIXES"))
SUFFIXES = tuple(config.get("PREFIXES"))
MAX_ACTIVE_TASKS = psutil.cpu_count()


# setup boto3
client_config = botocore.config.Config(max_pool_connections=MAX_ACTIVE_TASKS + 1)
s3 = boto3.resource("s3", config=client_config)  # create an S3 resource
BUCKET = s3.Bucket(BUCKET_NAME)  # instantiate an S3 bucket
CLIENT = s3.meta.client  # instantiate an S3 low-level client (thread safe)


async def main() -> None:
    """
    If there's a change in the directories
    delete/upload files from/to s3 bucket.
    """
    # perform initial setup
    init_set_up()

    # get the old index
    try:
        with open("logs/index.json", "r") as fp:
            old_index = json.load(fp)
    except OSError:
        old_index = {}

    # get the new index
    new_index = await compute_index()

    # if no change in index exit
    if new_index == old_index:
        sys.exit()

    # determine which objects to delete/upload
    data = compute_diff(new_index, old_index)

    # aynchronysly update S3 bucket (delete/update/upload files)
    await update_bucket(data)

    # save/overwrite the json index file with the fresh new index
    with open("logs/index.json", "w") as fp:
        json.dump(new_index, fp, indent=4)


def init_set_up() -> None:
    """
    Create `logs` folder if it doesn't exist.
    Setup basic logging.
    Exit if script is already running.
    """
    # ensure the logs folder exists
    os.makedirs("logs", exist_ok=True)

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


async def compute_index() -> dict[str, float]:
    """Compute index on every dict concurrently."""

    # execute coroutines concurrently and await for all results to come
    coros = [asyncio.to_thread(compute_dir_index, d) for d in DIRECTORIES]
    async with asyncio.TaskGroup() as tg:
        indexes = [tg.create_task(coro) for coro in coros]

    # merge all dicts into one index
    return {key: value for index in indexes for key, value in index.result().items()}


def compute_dir_index(root_dir: str) -> dict[str, float]:
    """
    Computes a directory's index of files and their last modified times.
    dir_path: absolute path to the root directory
    prefixes: list of prefixes to ignore
    suffixes: list of suffixes to ignore
    return: dictionary with files absolute paths and their last modified time
    """
    index = {}
    # traverse the path (os.walk is recursive)
    for current_dir_path, dir_names, file_names in os.walk(root_dir):
        # exclusion helper function
        permitted = lambda x: not (x.startswith(PREFIXES) or x.endswith(SUFFIXES))
        # copy of dir_names without excluded directories
        dir_names[:] = [d for d in dir_names if permitted(d)]
        # copy of file_names without excluded files
        file_names[:] = [f for f in file_names if permitted(f)]
        # loop through the file names in the current directory
        for file_name in file_names:
            # get the file's absolute path
            file_path = os.path.join(current_dir_path, file_name)
            if mtime := get_mtime(file_path):
                # record the file's last modification time
                index[str(file_path)] = mtime
    return index


def get_mtime(file_path: str) -> float | None:
    """Try to get the file's mtime."""
    try:
        # get the last modified time of the file
        return os.path.getmtime(file_path)
    except OSError:
        return None


def compute_diff(
    new_index: dict[str, float], old_index: dict[str, float]
) -> dict[str, list[str]]:
    """
    Computes the differences between the S3 bucket, new index and old index.
    new_index: newly computed directory index (dict)
    old_index: old directory index from a json file (dict)
    Return: dictionary of deleted/created/modified files
    """
    # get keys/files from indexes and the bucket
    new_index_files = set(new_index.keys())
    old_index_files = set(old_index.keys())
    bucket_files = set(f.key for f in BUCKET.objects.all())

    data = {}
    # files in the S3 bucket but not in the new index (deleted files) - sets difference
    data["deleted"] = list(bucket_files - new_index_files)
    # files in the new index but not in the S3 bucket (new files) - sets diference
    data["created"] = list(new_index_files - bucket_files)
    # files both in the old index and the new index (common files) - sets intersection
    common_files = old_index_files & new_index_files
    # common files with different last modified times (modified files)
    data["modified"] = [f for f in common_files if new_index[f] != old_index[f]]

    return data


async def update_bucket(data: dict[str, list[str]]) -> None:
    """
    Create the needed S3 resources and instances and
    delete/upload files concurrently.
    data: dictionary of deleted, new and modified files
    Return: None
    """
    # pair files with coresponding s3 function and sort to_upload by filesize
    to_delete = {key: delete_s3_object for key in data.get("deleted", [])}
    to_upload = data.get("created", []) + data.get("modified", [])
    to_upload.sort(key=lambda f: os.path.getsize(f))
    to_upload = {key: upload_s3_object for key in to_upload}
    objects = to_delete | to_upload

    # prepare coroutines for deletion and/or upload
    coros = [asyncio.to_thread(func, key) for key, func in objects.items()]

    # quick anonymous function for getting the number of current active tasks
    active_tasks = lambda: sum(1 for t in asyncio.all_tasks() if not t.done())

    # add tasks to group, the context will automatically await them
    start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        for coro in coros:
            while active_tasks() >= MAX_ACTIVE_TASKS:
                await asyncio.sleep(0.25)
            tg.create_task(coro)
    end = time.perf_counter()

    # log summary results
    logging.info(f"Processed {len(coros)} files. It took {end - start:.4f} seconds.")
    logging.info(60 * "-")


def delete_s3_object(key: str) -> None:
    try:
        response = CLIENT.delete_object(Bucket=BUCKET_NAME, Key=key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        logging.info(f"{key}: DELETE - {status}")
    except (ClientError, BotoCoreError) as e:
        logging.warning(f"{key}: FAIL")
        logging.error(e)


def upload_s3_object(key: str) -> None:
    try:
        # on success response is None
        CLIENT.upload_file(
            Filename=key,
            Bucket=BUCKET_NAME,
            Key=key,
            ExtraArgs={"StorageClass": STORAGE_CLASS},
        )
        logging.info(f"{key}: UPLOAD")
    except (ClientError, BotoCoreError) as e:
        logging.warning(f"{key}: FAIL")
        logging.error(e)


if __name__ == "__main__":
    asyncio.run(main())

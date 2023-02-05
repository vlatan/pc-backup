#! /usr/bin/env python3

import os
import sys
import json
import time
import boto3
import botocore
import psutil
import asyncio
import logging


# get config variables
with open("config.json", "r") as jsonfile:
    config = json.load(jsonfile)


DIRECTORIES = config.get("DIRECTORIES")
BUCKET_NAME = config.get("BUCKET_NAME")
STORAGE_CLASS = config.get("STORAGE_CLASS")
PREFIXES = tuple(config.get("PREFIXES"))
SUFFIXES = tuple(config.get("PREFIXES"))
MAX_POOL_SIZE = config.get("MAX_POOL_SIZE")


# setup boto3
client_config = botocore.config.Config(max_pool_connections=MAX_POOL_SIZE)
s3 = boto3.resource("s3", config=client_config)  # create an S3 resource
BUCKET = s3.Bucket(BUCKET_NAME)  # instantiate an S3 bucket
CLIENT = s3.meta.client  # instantiate an S3 low-level client (thread safe)


async def main() -> None:
    """
    If there's a change in the directories
    delete/upload files from/to s3 bucket.
    return: None
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
    try:
        os.makedirs("logs")
    except OSError:
        pass

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
    return: True if it's running, False otherwise
    """
    # iterate through all the current processes
    for q in psutil.process_iter():
        # if it's a python process
        if q.name().startswith("python"):
            # if it's this script but with different PID
            if (
                len(q.cmdline()) > 1
                and sys.argv[0] in q.cmdline()[1]
                and q.pid != os.getpid()
            ):
                return True
    return False


async def compute_index() -> dict[str, float]:
    """Compute index on every dict concurrently."""

    # gather compute_dir_index tasks
    tasks = []
    for directory in DIRECTORIES:
        coroutine = asyncio.to_thread(compute_dir_index, directory)
        tasks.append(coroutine)
    # execute tasks concurrently and await for all results to come
    indexes = await asyncio.gather(*tasks)

    # merge all dicts into one index
    new_index = {}
    for index in indexes:
        new_index.update(index)

    return new_index


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
    Computes the differences between the S3 bucket, the
    new index and the old index.
    new_index: newly computed directory index (dict)
    old_index: old directory index from a json file (dict)
    BUCKET: S3 bucket boto3 instance.
    return: dictionary of deleted/created/modified files
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
    return: None
    """
    # prepare tasks for deletion/update/upload
    tasks = []
    # files to delete
    for key in data.get("deleted", []):
        coroutine = asyncio.to_thread(delete_s3_object, key)
        tasks.append(coroutine)
    # files to update/upload
    for key in data.get("created", []) + data.get("modified", []):
        coroutine = asyncio.to_thread(put_s3_object, key)
        tasks.append(coroutine)

    start = time.perf_counter()
    # prepare empty list for final results and chunks of files for processing
    results, chunks = [], list(divide_list_in_chunks(tasks, MAX_POOL_SIZE - 10))
    # execute tasks in MAX_POOL_SIZE chunks
    for chunk in chunks:
        # tasks in each chunk run concurrently
        results += await asyncio.gather(*chunk)
    end = time.perf_counter()

    # log summary results
    logging.info(f"Processed {len(results)} files. It took {end - start:.4f} seconds.")
    logging.info(60 * "-")


def delete_s3_object(key):
    response = CLIENT.delete_object(Bucket=BUCKET_NAME, Key=key)
    log_file_status(key, response)


def put_s3_object(key):
    response = CLIENT.put_object(
        Body=key,
        Bucket=BUCKET_NAME,
        Key=key,
        StorageClass=STORAGE_CLASS,
    )
    log_file_status(key, response)


def log_file_status(key: str, response: dict) -> None:
    """Log file delete or put response status."""
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    logging.info(f"{key}: {status}")


def divide_list_in_chunks(lst, n):
    """Loop till length of list with step n, yield chunk of size n."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


if __name__ == "__main__":
    asyncio.run(main())

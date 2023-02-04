#! /usr/bin/env python3

import os
import sys
import json
import boto3
import psutil
import asyncio
import time
from pathlib import Path


# get config variables
with open("config.json", "r") as jsonfile:
    config = json.load(jsonfile)

DIRECTORIES = config.get("DIRECTORIES")
BUCKET_NAME = config.get("BUCKET_NAME")
STORAGE_CLASS = config.get("STORAGE_CLASS")
PREFIXES = tuple(config.get("PREFIXES"))
SUFFIXES = tuple(config.get("PREFIXES"))

s3 = boto3.resource("s3")  # create an S3 resource
BUCKET = s3.Bucket(BUCKET_NAME)  # instantiate an S3 bucket
CLIENT = s3.meta.client  # instantiate an S3 low-level client


def main():
    """
    If there's a change in the directories
    delete/upload files from/to s3 bucket.
    return: None
    """
    # if this script/file is already running exit
    if is_running():
        sys.exit()

    # ensure the logs folder exists
    try:
        os.makedirs("logs")
    except OSError:
        pass

    # get the old index
    try:
        with open("logs/index.json", "r") as fp:
            old_index = json.load(fp)
    except OSError:
        old_index = {}

    # compute the current/new index
    new_index = {}
    for directory in DIRECTORIES:
        new_index.update(compute_dir_index(directory))

    # if no change in index exit
    if new_index == old_index:
        sys.exit()

    # aynchronysly update S3 bucket (delete/update/upload files)
    asyncio.run(update_bucket(new_index, old_index))

    # save/overwrite the json index file with the fresh new index
    with open("logs/index.json", "w") as fp:
        json.dump(new_index, fp, indent=4)


def is_running():
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


def compute_dir_index(root_dir: str) -> dict:
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
            file_path = Path(current_dir_path, file_name)
            if mtime := get_mtime(file_path):
                # record the file's last modification time
                index[str(file_path)] = mtime
    return index


def get_mtime(file_path: str) -> str:
    """Try to get the file's mtime."""
    try:
        # try to open the file to make sure
        # it's not in the middle of a copy/paste operation
        with open(file_path, "r"):
            # get the last modified time of the file
            return os.path.getmtime(file_path)
    except OSError:
        return None


async def update_bucket(new_index, old_index):
    """
    Create the needed S3 resources and instances and
    delete/upload files concurrently.
    data: dictionary of deleted, new and modified files
    return: None
    """

    # determine which objects to delete/upload
    data = compute_diff(new_index, old_index)

    # prepare tasks for deletion/update/upload
    tasks = []
    for key in data.get("deleted", []):
        tasks.append(delete_s3_object(key))
    for key in data.get("created", []) + data.get("modified", []):
        tasks.append(upload_s3_object(key))

    # execute tasks concurrently
    return await asyncio.gather(*tasks)


def compute_diff(new_index, old_index):
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


async def delete_s3_object(key):
    """Makse the client.delete_object method asynchronous."""
    await asyncio.to_thread(CLIENT.delete_object, Bucket=BUCKET_NAME, Key=key)


async def upload_s3_object(key):
    """Makse the client.upload_file method asynchronous."""
    await asyncio.to_thread(
        CLIENT.upload_file,
        Filename=key,
        Bucket=BUCKET_NAME,
        Key=key,
        ExtraArgs={"StorageClass": STORAGE_CLASS},
    )


def sleep_function():
    time.sleep(1)
    print("Upload done")


if __name__ == "__main__":
    main()

import boto3
import asyncio
import logging
import hashlib
import botocore.config
from pathlib import Path
from botocore.exceptions import ClientError, BotoCoreError

import config as cfg
from utils import init_set_up, permitted


# setup boto3
client_config = botocore.config.Config(max_pool_connections=cfg.MAX_ACTIVE_TASKS + 1)
s3 = boto3.resource("s3", config=client_config)  # create an S3 resource
s3_bucket = s3.Bucket(cfg.BUCKET_NAME)  # instantiate an S3 bucket
s3_client = s3.meta.client  # instantiate an S3 low-level client (thread safe)


async def main() -> None:
    """Sync directories with s3 bucket."""
    # perform initial setup
    init_set_up()

    # compute local file index
    index = await compute_index()

    # aynchronysly update S3 bucket (delete/update/upload files)
    await update_bucket(index)


async def compute_index() -> set[str]:
    """
    Compute index on every directory concurrently.
    Return: Set of local absolute file paths.
    """

    # execute coroutines concurrently and await for all results to come
    coros = [asyncio.to_thread(compute_dir_index, Path(d)) for d in cfg.DIRECTORIES]
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(coro) for coro in coros]

    # merge all sets into one
    return {key for index in tasks for key in index.result()}


def compute_dir_index(root_dir: Path) -> set[str]:
    """
    Compute a directory's index of files.
    root_dir: Absolute path to the root directory.
    Return: Set of local absolute file paths.
    """
    index = set()
    # traverse the path recursively
    for current_root, dir_names, file_names in root_dir.walk():
        # copy of dir_names without excluded directories
        dir_names[:] = [d for d in dir_names if permitted(d)]
        # copy of file_names without excluded files
        file_names[:] = [f for f in file_names if permitted(f)]
        # loop through the file names in the current directory
        for file_name in file_names:
            # add the file's absolute path to set
            if (file_path := current_root / file_name).exists():
                index.add(str(file_path))
    return index


async def update_bucket(index: set[str]) -> None:
    """
    Examine which files to upload/delete and do so in async.
    index: Set of local absolute file paths.
    Return: None.
    """

    # which files to delete and/or upload
    to_upload, to_delete = index, []

    # compare the bucket objects to local files in the index
    for obj in s3_bucket.objects.all():

        # if the bucket object is not in the index, remove from bucket
        if (key := f"/{obj.key}") not in index:
            to_delete.append({"Key": obj.key})
            continue

        # if in the meantime the local file was removed,
        # remove from bucket and do not reupload
        if not (file_path := Path(key)).exists():
            to_delete.append({"Key": obj.key})
            to_upload.discard(key)
            continue

        # if the object has a different size or a different etag, reupload
        file_size = file_path.stat().st_size
        if obj.size != file_size or obj.e_tag.strip('"') != etag(key):
            continue

        # otherwise do not reupload this file
        to_upload.discard(key)

    # files can be deleted in batches of max 1000 files per batch
    to_delete = [to_delete[i : i + 1000] for i in range(0, len(to_delete), 1000)]
    coros = [asyncio.to_thread(bulk_delete_s3_objects, lst) for lst in to_delete]
    coros += [asyncio.to_thread(upload_s3_object, key) for key in to_upload]

    # quick anonymous function for getting the number of current active tasks
    active_tasks = lambda: sum(1 for t in asyncio.all_tasks() if not t.done())

    # add tasks to group, the context will automatically await the group
    # upload/delete files in parallel up to the number of MAX_ACTIVE_TASKS
    async with asyncio.TaskGroup() as tg:
        for coro in coros:
            while active_tasks() >= cfg.MAX_ACTIVE_TASKS:
                await asyncio.sleep(0.25)
            tg.create_task(coro)


def bulk_delete_s3_objects(keys: list[dict[str, str]]) -> None:
    """
    Delete multiple s3 objects with one HTTP request.
    The limit is 1000 objects per request.
    keys: list of dicts - {"Key": str}
    """
    try:
        s3_client.delete_objects(Bucket=cfg.BUCKET_NAME, Delete={"Objects": keys})
    except (ClientError, BotoCoreError) as e:
        logging.error(e)


def upload_s3_object(key: str) -> None:
    """Upload file to S3 bucket."""
    try:
        # on success response is None
        s3_client.upload_file(
            Filename=key,
            Bucket=cfg.BUCKET_NAME,
            Key=key.lstrip("/"),
            ExtraArgs={"StorageClass": cfg.STORAGE_CLASS},
        )
    except (ClientError, BotoCoreError, OSError) as e:
        logging.error(e)


def etag(filepath: str) -> str:
    with open(filepath, "rb") as f:
        return hashlib.file_digest(f, "md5").hexdigest()


if __name__ == "__main__":
    asyncio.run(main())

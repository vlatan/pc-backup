#! /usr/bin/env python3

import os
import sys
import psutil
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from helpers import *
from variables import *


def handle_object(args):
    """ Delete/upload a file from/to an S3 bucket.
        args: [client, bucket_name, key, filename, storage_class, delete]
        return: True if the file was deleted/uploaded """

    client, bucket_name, key = args[0], args[1], args[2]
    filename, storage_class, delete = args[3], args[4], args[5]
    if delete:
        client.delete_object(Bucket=bucket_name,
                             Key=key)
    else:
        client.upload_file(Filename=filename,
                           Bucket=bucket_name,
                           Key=key,
                           ExtraArgs={'StorageClass': storage_class})
    return True


def execute_threads(super_args):
    """ Delete/upload files concurrently each in different thread.
        super_args: A list of lists each containing args for handle_object(args)
        return: None """

    max_workers = max(len(super_args), os.cpu_count() + 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_keys = {}
        for i in range(len(super_args)):
            k = executor.submit(handle_object, super_args[i])
            future_keys[k] = [super_args[i][2], super_args[i][5]]

        uploaded, deleted = 0, 0
        # inspect completed (finished or canceled) futures/threads
        for future in as_completed(future_keys):
            key, delete = future_keys[future][0], future_keys[future][1]
            try:
                future.result()
                if delete:
                    deleted += 1
                    print(f'DELETED: {key}.')
                else:
                    uploaded += 1
                    print(f'UPLOADED: {key}.')
            except Exception as e:
                print(f'FILE: {key}.')
                print(f'EXCEPTION: {e}.')

    time_now = datetime.now().strftime('%d.%m.%Y, %H:%M:%S')
    print('-' * 53)
    print(f'Uploaded: {uploaded}. Deleted: {deleted}.', end=' ')
    print(f'Time: {time_now}.\n\n')


def aws_sdk_sync(new_index, old_index, user_root,
                 bucket_name, json_index_file):
    """ If there's a change in the file indexes create the needed S3 resources
        and instances and delete/upload files concurrently.
        new_index: the new index of files
        old_index: the old index of files
        user_root: the user's home path
        bucket_name: S3 bucket name
        json_index_file: path to the json index file
        return: None """

    # create an S3 resource
    s3 = boto3.resource('s3')

    # instantiate an S3 bucket
    bucket = s3.Bucket(bucket_name)

    # determine which objects to delete/upload
    data = compute_diff(new_index, old_index, bucket)

    # build a list of files/keys that need to be handled
    keys_to_handle = []
    for value in data.values():
        keys_to_handle += value

    # if there are keys/files to be handled
    if keys_to_handle:

        # if there's a difference in the indexes
        if new_index != old_index:
            # save/overwrite the json index file with the fresh new index.
            # we're overwriting this early (before the job below finishes)
            # because if there are many and/or huge files for upload/deletion
            # that can take quite some time (longer than the cron interval),
            # therefore this script will run again before it finishes.
            # depending on the cron interval that can happen again and again
            # which will upload/delete the same files over and over again.
            save_json(json_index_file, new_index)

        # instantiate an S3 low-level client
        client = s3.meta.client

        # construct a list of lists filled with parameters needed
        # for handling every key so we can easily map the parameters
        # for all the keys to the function that handles objects
        super_args = []
        for key in data['deleted']:
            super_args.append([client, bucket_name, key, None, None, True])
        for key in data['created'] + data['modified']:
            super_args.append([client, bucket_name, key,
                               f'{user_root}/{key}', 'STANDARD_IA', False])

        # delete/upload files concurrently
        execute_threads(super_args)


def is_running():
    """ Check if this script is already running.
        return: True if it's running, False otherwise """

    # iterate through all the current processes
    for q in psutil.process_iter():
        # if it's a python process
        if q.name().startswith('python'):
            # if it's this script but with different PID
            if len(q.cmdline()) > 1 and sys.argv[0] in q.cmdline()[1] and q.pid != os.getpid():
                return True
    return False


def main():
    # if this script is NOT already running
    if not is_running():

        # the current/new index
        new_index = compute_dir_index(user_root, dirs_to_sync,
                                      exclude_prefixes, exclude_suffixes)
        # the old index
        old_index = read_json(json_index_file)

        # synchronize with S3
        aws_sdk_sync(new_index, old_index, user_root,
                     bucket_name, json_index_file)


if __name__ == '__main__':
    main()

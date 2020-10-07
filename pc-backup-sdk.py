#! /usr/bin/env python3

import os
import sys
import json
import psutil
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from constants import *


def compute_dir_index(path, dirs_to_sync, prefixes, suffixes):
    """ path: path to the root directory.
        dirs_to_sync: directories we want to sync within the path.
        prefixes: tuple of prefixes to ignore.
        suffixes: tuple of suffixes to ignore.
        returns a dictionary with files and their last modified time'. """
    index = {}
    # traverse the path
    for root, dirs, files in os.walk(path):
        # if first level directory
        if root == path:
            # ignore files in the first level root
            files[:] = []
            # include only folders we want to sync
            dirs[:] = [d for d in dirs if d in dirs_to_sync]
        else:
            # exclude hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(prefixes)]
            # exclude prefixes (hidden files)
            # and suffixes (certain extensions)
            files[:] = [f for f in files
                        if not f.startswith(prefixes)
                        and not f.endswith(suffixes)]
        # loop through the files in the current directory
        for f in files:
            # get the file path relative to the dir
            file_path = os.path.relpath(os.path.join(root, f), path)
            # get the last modified time of that file
            mtime = os.path.getmtime(os.path.join(path, file_path))
            # put the file in the index
            index[file_path] = mtime
    return index


def compute_diff(new_index, old_index, bucket):
    """ Computes the differences between the bucket, the
        new index and the old index.
        new_index: newly computed directory index (dict).
        old_index: old directory index from a json file (dict).
        bucket: S3 bucket boto3 instance.
        Returns: Dictionary of deleted/created/modified files. """

    # get keys/files from indexes and the bucket
    new_index_files = set(new_index.keys())
    old_index_files = set(old_index.keys())
    bucket_files = set(f.key for f in bucket.objects.all())

    data = {}
    # files present in the S3 bucket but not in the new index
    data['deleted'] = list(bucket_files - new_index_files)
    # files present in the new index but not in the S3 bucket
    data['created'] = list(new_index_files - bucket_files)
    # files present both in the new index and the old index
    # but with different last modified times
    common_files = old_index_files.intersection(new_index_files)
    data['modified'] = [f for f in common_files if new_index[f] != old_index[f]]

    return data


def read_json(json_file):
    """ json_file: a path to json file to read.
        Returns the content of the file (dict).
        If there's no such file it returns an empty dict. """
    # try to read the old index json file
    try:
        with open(json_file, 'r') as f:
            old_index = json.load(f)
    # if there's no such file the old_index is an empty dict
    except IOError:
        old_index = {}
    return old_index


def save_json(json_file, new_index):
    """ json_file: a path to json file to save/overwrite.
        new_index: the new content/index for the file (dict).
        Saves/dumps the new index into a json file.
        Returns: None. """
    with open(json_file, 'w') as f:
        json.dump(new_index, f, indent=4)


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


def aws_sdk_sync(new_index, old_index, user_dir,
                 bucket_name, json_index_file):
    """ If there's a change in the file indexes create the needed S3 resources
        and instances and delete/upload files concurrently.
        new_index: the new index of files
        old_index: the old index of files
        user_dir: the user's home path
        bucket_name: S3 bucket name
        json_index_file: path to the json index file
        return: None """

    # if there's a difference in the indexes
    if new_index != old_index:

        # create an S3 resource
        s3 = boto3.resource('s3')

        # instantiate an S3 bucket
        bucket = s3.Bucket(bucket_name)

        # determine which objects to delete/upload
        data = compute_diff(new_index, old_index, bucket)

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
                               f'{user_dir}/{key}', 'STANDARD_IA', False])

        # delete/upload files concurrently
        execute_threads(super_args)

        # save/overwrite the json index file with the fresh new index
        save_json(json_index_file, new_index)


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
        # compute the current/new index
        new_index = compute_dir_index(USER_DIR, DIRS, PREFIXES, SUFFIXES)
        # get the old index
        old_index = read_json(INDEX_FILE)
        # synchronize with S3
        aws_sdk_sync(new_index, old_index, USER_DIR, BUCKET_NAME, INDEX_FILE)


if __name__ == '__main__':
    main()

#! /usr/bin/env python3

import os
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from helpers import *
from variables import *


def handle_object(args):
    """ Delete/upload a file from/to an S3 bucket
        args: [client, bucket_name, key, filename, storage_class, delete]
        client: an instance of a boto3 S3 client object
        bucket_name: the name of the S3 bucket
        key: an object key
        filename: local path to file
        storage_class: the storage class of the object
        delete: if True delete object, if False upload file
        return: True """
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


def aws_sdk_sync(new_index, old_index, user_root,
                 bucket_name, json_index_file):

    # if there's a difference in the indexes
    if new_index != old_index:

        # create an S3 resource
        s3 = boto3.resource('s3')

        # instantiate an S3 bucket
        bucket = s3.Bucket(bucket_name)

        # determine which objects to delete/upload
        data = compute_diff(new_index, old_index, bucket)

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

        # construct a list of lists filled with parameters for every key
        # so we can easily map the parameters for all the keys
        # to the function that handles objects
        args = []
        for key in data['deleted']:
            args.append([client, bucket_name, key, None, None, True])
        for key in data['created'] or data['modified']:
            args.append([client, bucket_name, key,
                         f'{user_root}/{key}', 'STANDARD_IA', False])

        # delete/upload files concurrently
        max_workers = max(len(args), os.cpu_count() + 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_keys = {executor.submit(handle_object, args[i]):
                           [args[i][2], args[i][5]] for i in range(len(args))}

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


if __name__ == '__main__':

    # the current/new index
    new_index = compute_dir_index(user_root, dirs_to_sync,
                                  exclude_prefixes, exclude_suffixes)

    # the old index
    old_index = read_json(json_index_file)

    # synchronize
    aws_sdk_sync(new_index, old_index, user_root,
                 bucket_name, json_index_file)

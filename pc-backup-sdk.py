#! /usr/bin/env python3

import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from variables import *
from helpers import *


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


if __name__ == '__main__':

    # the directory's current/new index
    new_index = compute_dir_index(
        user_root, dirs_to_sync, exclude_prefixes, exclude_suffixes)

    # the directory's old index
    old_index = read_json(json_index_file)

    # if there's a difference in the indexes (old and new)
    if new_index != old_index:

        # create an S3 resource
        s3 = boto3.resource('s3')

        # instantiate an S3 bucket
        bucket = s3.Bucket(bucket_name)

        # which objects to delete/upload
        data = compute_diff(new_index, old_index, bucket)

        # save/overwrite the json index file with the fresh new index
        # we're overwriting this early (before the job below finishes)
        # because if there are many and/or huge files for upload/deletion
        # that can take quite some time (longer than the cron interval)
        # therefore the cron will run this script simultaneously many times
        # which will upload/delete the same files over and over
        save_json(json_index_file, new_index)

        # instantiate an S3 low-level client
        client = s3.meta.client

        # construct a list of lists filled with parameters for every S3 key
        # so we can later easily map the parameters
        # to the function that handles objects
        args = []
        for key in data['deleted']:
            args.append([client, bucket_name, key, None, None, True])
        for key in data['created'] or data['modified']:
            args.append([client, bucket_name, key,
                         f'{user_root}/{key}', 'STANDARD_IA', False])

        # delete/upload files concurrently
        with ThreadPoolExecutor(max_workers=len(args) + 10) as executor:
            future_keys = {executor.submit(handle_object, args[i]):
                           [args[i][2], args[i][5]] for i in range(len(args))}

            uploaded, deleted = 0, 0
            for future in as_completed(future_keys):
                key, delete = future_keys[future][0], future_keys[future][1]
                try:
                    output = future.result()
                    if delete:
                        deleted += 1
                    else:
                        uploaded += 1
                except Exception as e:
                    print(f'FILE: {key}.')
                    print(f'EXCEPTION: {e}.')
                    print('-' * 53)

        time_now = datetime.now().strftime('%d.%m.%Y at %H:%M:%S')
        print(f'Uploaded:{uploaded}. Deleted:{deleted}. Time:{time_now}.')
        print('=' * 53)

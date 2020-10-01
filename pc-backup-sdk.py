#! /usr/bin/env python

import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        # create an instance of the S3 bucket object
        bucket = s3.Bucket(bucket_name)

        # which objects to delete/upload
        data = compute_diff(new_index, old_index, bucket)

        # instantiate an S3 client object
        client = boto3.client('s3')

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

            deleted, uploaded, failed = 0, 0, 0
            for future in as_completed(future_keys):
                key, job = future_keys[future][0], future_keys[future][1]
                try:
                    output = future.result()
                    if job:
                        deleted += 1
                    else:
                        uploaded += 1
                except Exception as e:
                    failed += 1
                    print(f'{key} generated an exception {e}')

        print(f'Deleted: {deleted}. Uploaded: {uploaded}. Failed: {failed}')

        # save/overwrite the json index file
        save_json(json_index_file, new_index)

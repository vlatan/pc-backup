#! /usr/bin/env python

import logging
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
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
        delete: True -> delete object, False -> upload file
        return: None. """
    client, bucket_name, key = args[0], args[1], args[2]
    filename, storage_class, delete = args[3], args[4], args[5]
    try:
        if delete:
            client.delete_object(Bucket=bucket_name,
                                 Key=key)
        else:
            client.upload_file(Filename=filename,
                               Bucket=bucket_name,
                               Key=key,
                               ExtraArgs={'StorageClass': storage_class})
    except ClientError as e:
        logging.error(e)


if __name__ == '__main__':

    # the directory's current/new index
    new_index = compute_dir_index(
        user_root, dirs_to_sync, exclude_prefixes, exclude_suffixes)

    # the directory's old index
    old_index = read_json(json_index_file)

    # if there's a difference in the indexes (old and new)
    if new_index != old_index:

        # configure logging, log_file is defined in variables.py
        logging.basicConfig(filename=log_file, level=logging.DEBUG)

        # create S3 resource
        s3 = boto3.resource('s3')
        # create an instance of the bucket
        bucket = s3.Bucket(bucket_name)

        # which objects to delete/upload
        data = compute_diff(new_index, old_index, bucket)

        client = boto3.client('s3')

        args = []
        for key in data['deleted']:
            args.append([client, bucket_name, key, None, None, True])
        for key in data['created'] or data['modified']:
            args.append([client, bucket_name, key,
                         f'{user_root}/{key}', 'STANDARD_IA', False])

        # delete/upload files in parallel
        with ThreadPoolExecutor(max_workers=len(args)) as executor:
            executor.map(handle_object, args)

        num_deleted = len(data['deleted'])
        num_uploaded = len(data['created'] or data['modified'])

        logging.info(f'Deleted: {num_deleted}. Uploaded: {num_uploaded}.')

        # save/overwrite the json index file
        save_json(json_index_file, new_index)

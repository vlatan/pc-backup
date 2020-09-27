#!/usr/bin/env python

import boto3
from variables import *
from helpers import *


def bulk_delete_files(files, bucket):
    # remove objects from S3 bucket
    bucket.delete_objects(
        Delete={
            'Objects': [{'Key': key} for key in files],
            'Quiet': True
        }
    )


def upload_file(filename, key, bucket, storage_class='STANDARD_IA'):
    # upload object to S3 bucket
    bucket.upload_file(Filename=filename,
                       Key=key,
                       ExtraArgs={'StorageClass': storage_class})


def synchronize(user_root, data, bucket, storage_class='STANDARD_IA'):
    # remove files from S3 bucket
    if data['deleted']:
        bulk_delete_files(data['deleted'], bucket)
        print('Deleted:')
        for f in data['deleted']:
            print(f)

    # upload file to S3 bucket
    if data['created'] or data['modified']:
        print('Uploaded:')
        for key in data['created'] + data['modified']:
            filename = f'{user_root}/{key}'
            upload_file(filename, key, bucket, storage_class)
            print(key)


if __name__ == '__main__':

    # the directory's current/new index
    new_index = compute_dir_index(
        user_root, dirs_to_sync, exclude_prefixes, exclude_suffixes)

    # the directory's old index
    old_index = read_json(json_index_file)

    # if there's a difference in the indexes (old and new)
    if new_index != old_index:

        # create S3 resource
        s3 = boto3.resource('s3')
        # create an instance of the bucket
        bucket = s3.Bucket(bucket_name)

        # objects to delete/upload
        data = compute_diff(new_index, old_index, bucket)

        # delete/upload objects
        synchronize(user_root, data, bucket)

        # save/overwrite the json index file
        save_json(json_index_file, new_index)

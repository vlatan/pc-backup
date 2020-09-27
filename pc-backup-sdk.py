#!/usr/bin/env python

from datetime import datetime
import boto3
from variables import *
from helpers import *


def compute_diff(new_index, old_index, bucket):
    """ Computes the differences between the bucket, the
        new index and the old index.
        new_index: newly computed directory index (dict).
        old_index: old directory index from a json file (dict).
        bucket: S3 bucket boto3 instance.
        Returns: Dictionary of deleted/created/modified files. """

    data = {}
    # get the bucket files
    bucket_files = set(f.key for f in bucket.objects.all())
    # get keys from indexes, which are in fact the files
    new_index_files = set(new_index.keys())
    old_index_files = set(old_index.keys())
    # files present in the S3 bucket but not in the new index
    data['deleted'] = list(bucket_files - new_index_files)
    # files present in the new index but not in the S3 bucket
    data['created'] = list(new_index_files - bucket_files)
    # files present both in the new index and the old index
    # but with different last modified times
    common_files = old_index_files.intersection(new_index_files)
    data['modified'] = [f for f in common_files if new_index[f] != old_index[f]]

    return data


def bulk_delete_files(files, bucket):
    # remove objects from S3 bucket
    bucket.delete_objects(
        Delete={
            'Objects': [{'Key': key} for key in files],
            'Quiet': True
        }
    )


def upload_file(file, bucket, storage_class='STANDARD_IA'):
    # upload object to S3 bucket
    bucket.upload_file(Filename=file,
                       Key=file,
                       ExtraArgs={'StorageClass': storage_class})


def synchronize(data, bucket, storage_class):
    # remove files from S3 bucket
    bulk_delete_files(data['deleted'], bucket)

    # upload file to S3 bucket
    for file in data['created'] + data['modified']:
        upload_file(file, bucket, storage_class)


if __name__ == '__main__':

    # the directory's current/new index
    new_index = compute_dir_index(
        dir_path, dirs_to_sync, exclude_prefixes, exclude_suffixes)

    # the directory's old index
    old_index = read_json(json_index_file)

    # if there's a difference in the indexes (old and new)
    if new_index != old_index:

        # create S3 resource
        s3 = boto3.resource('s3')
        # create an instance of the bucket
        bucket = s3.Bucket(bucket_name)
        # get the bucket files
        bucket_files = [f.key for f in bucket.objects.all()]

        # objects to delete/upload
        data = compute_diff(new_index, old_index, bucket_files)

        # delete/upload objects
        synchronize(data, bucket)

        # save/overwrite the json index file
        save_json(json_index_file, new_index)

#!/usr/bin/env python

import os
import json


def compute_dir_index(path, dirs_to_sync,
                      exclude_prefixes=(), exclude_suffixes=()):
    """ path: path to the root directory.
        dirs_to_sync: directories we want to sync within the path.
        exclude_prefixes: tuple of prefixes to ignore.
        exclude_suffixes: tuple of suffixes to ignore.
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
            dirs[:] = [d for d in dirs if not d.startswith(exclude_prefixes)]
            # exclude prefixes (hidden files)
            # and suffixes (certain extensions)
            files = [f for f in files
                     if not f.startswith(exclude_prefixes)
                     and not f.endswith(exclude_suffixes)]
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

#!/usr/bin/env python

import os
import json
from datetime import datetime
from paths import *


def compute_dir_index(path, exclude_prefixes=(), exclude_suffixes=()):
    """ path: path to the directory.
        exclude_prefixes: tuple of prefixes to ignore.
        exclude_suffixes: tuple of suffixes to ignore.
        Returns a dictionary with 'file: last modified time timestamp'. """
    index = {}
    # traverse the dir
    for root, dirs, files in os.walk(path):
        # exclude hidden directories
        dirs[:] = [d for d in dirs if not d.startswith(exclude_prefixes)]
        # exclude prefixes (hidden files) or/and suffixes (certain extensions)
        files = [f for f in files
                 if not f.startswith(exclude_prefixes)
                 and not f.endswith(exclude_suffixes)]
        # loop through the files in the current directory
        for f in files:
            # get the file path relative to the dir
            file_path = os.path.relpath(os.path.join(root, f), path)
            # get the last modified time of that file
            mtime = os.path.getmtime(os.path.join(path, file_path))
            # put them in the index
            index[file_path] = mtime

    # return a dict of files as keys and
    # last modified time as their values
    return index


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


def build_sync_excludes(exclude_prefixes=(), exclude_suffixes=()):
    """ exclude_prefixes: tuple of prefixes to exclude.
        exclude_suffixes: tuple of suffixes to exclude.
        Returns a string with '--exclude' arguments if any.
        If there are prefixes and/or suffixes to exclude there's
        on purpose an empty space at the end of the string. """
    exclude = ''
    for prefix in exclude_prefixes:
        exclude += f'--exclude "{prefix}*" '
    for suffix in exclude_suffixes:
        exclude += f'--exclude "*{suffix}" '
    return exclude


def aws_sync(dir_path, bucket_path, exclude):
    """ dir_path: the local path to the directory (string).
        bucket_path: the bucket path to the directory (string).
        exclude: --exclude arguments if any (string).
        It constructs and runs an aws sync command.
        Returns: None. """
    sync = '/usr/local/bin/aws s3 sync '
    sync += f'"{dir_path}/" "{bucket_path}/" {exclude}'
    sync += '--storage-class STANDARD_IA --delete --quiet'
    os.system(sync)


def print_statements(statements_to_print):
    """ statements_to_print: a list of statements(strings) to print.
        Prints statements.
        Returns: None. """
    if statements_to_print:
        for statement in statements_to_print:
            print(statement)
        print('-' * 53)


if __name__ == "__main__":
    # The following 6 variables are defined in a separate paths.py file:
    # root, bucket, dirs, index_path, exclude_prefixes, exclude_suffixes.
    # These variables are sensitive and unique to your environment.
    # Read the README.md file.

    # construct filenames with lowercase letters and no white spaces
    json_names = [name.replace(' ', '-').lower() + '.json' for name in dirs]

    # prepare '--exclude' arguments if any for the aws sync command
    exclude = build_sync_excludes(exclude_prefixes, exclude_suffixes)

    # possible statement to print for logging purposes
    statements_to_print = []

    for i in range(len(dirs)):
        # the local path to the directory
        dir_path = root + dirs[i]
        # the bucket path to the directory
        bucket_path = bucket + dirs[i]

        # the directory's current/new index
        new_index = compute_dir_index(
            dir_path, exclude_prefixes, exclude_suffixes)

        # the old index json file
        json_file = index_path + json_names[i]
        # the directory's old index
        old_index = read_json(json_file)

        # if there's a difference in the indexes (old and new)
        if new_index != old_index:
            # sync this folder with the same folder in the bucket
            aws_sync(dir_path, bucket_path, exclude)

            # current date and time
            time_now = datetime.now().strftime('%d.%m.%Y at %H:%M:%S')
            # append a statement to print later
            statements_to_print.append(f"Synced '{dirs[i]}' on {time_now}.")

            # save/overwrite the json file with the new index
            save_json(json_file, new_index)

    print_statements(statements_to_print)

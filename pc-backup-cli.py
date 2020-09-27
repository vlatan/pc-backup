#!/usr/bin/env python

import os
from datetime import datetime
from variables import *
from helpers import *


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


def which_dirs(data):
    # figure out # within which directories there's a change
    changed_files = []
    for value in data.values():
        changed_files.extend(value)
    return list(set(f.split('/')[0] for f in changed_files))


def print_statements(statements_to_print):
    """ statements_to_print: a list of statements(strings) to print.
        Prints statements.
        Returns: None. """
    if statements_to_print:
        for statement in statements_to_print:
            print(statement)
        print('-' * 53)


if __name__ == "__main__":
    # The following 6 variables are defined in a separate variables.py file:
    # 1. user_root, 2. bucket_name, 3. dirs_to_sync, 4. json_index_file,
    # 5. exclude_prefixes, 6. exclude_suffixes.
    # These variables are sensitive and unique to your environment.
    # Read the README.md file.

    # the directory's current/new index
    new_index = compute_dir_index(
        user_root, dirs_to_sync, exclude_prefixes, exclude_suffixes)

    # the directory's old index
    old_index = read_json(json_index_file)

    # if there's a difference in the indexes (old and new)
    if new_index != old_index:

        # possible statement to print for logging purposes
        statements_to_print = []

        # build exclude string
        exclude = build_sync_excludes(exclude_prefixes, exclude_suffixes)

        # objects to delete/upload
        data = compute_diff(new_index, old_index, bucket=None)

        # within which directories there's a change
        changed_dirs = which_dirs(data)

        for i in range(len(changed_dirs)):
            # the local path to the directory
            dir_path = f'{user_root}/{changed_dirs[i]}'
            # the bucket path to the directory
            bucket_path = f's3://{bucket_name}/{changed_dirs[i]}'
            # sync this folder with the same folder in the bucket
            aws_sync(dir_path, bucket_path, exclude)
            # current date and time
            time_now = datetime.now().strftime('%d.%m.%Y at %H:%M:%S')
            # append a statement to print later
            statements_to_print.append(
                f"Synced '{changed_dirs[i]}' on {time_now}.")

        # save/overwrite the json file with the new index
        save_json(json_index_file, new_index)

        # print statements if any
        print_statements(statements_to_print)

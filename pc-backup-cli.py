#! /usr/bin/env python

import subprocess
from datetime import datetime
from variables import *
from helpers import *


def build_sync_excludes(exclude_prefixes=(), exclude_suffixes=()):
    """ exclude_prefixes: tuple of prefixes to exclude.
        exclude_suffixes: tuple of suffixes to exclude.
        Returns a list with '--exclude' arguments if any. """
    exclude = []
    for prefix in exclude_prefixes:
        exclude += ['--exclude', f'{prefix}*', '--exclude', f'*/{prefix}*']
    for suffix in exclude_suffixes:
        exclude += ['--exclude', f'*{suffix}']
    return exclude


def aws_sync(dir_path, bucket_path, exclude):
    """ dir_path: the local path to the directory (string).
        bucket_path: the bucket path to the directory (string).
        exclude: --exclude arguments if any (list).
        It constructs and runs an aws sync command.
        Returns: None. """

    # prepare the sync command
    sync = ['/usr/local/bin/aws', 's3', 'sync']
    sync += [f'{dir_path}/', f'{bucket_path}/'] + exclude
    sync += ['--storage-class', 'STANDARD_IA', '--delete', '--quiet']

    # execute the sync command
    subprocess.run(sync, timeout=80, check=True)


def which_dirs(data):
    """ Figures out within which directories there's a change.
        data: dictionary of deleted/created/modified files.
        returns: list of directories with modified content. """
    changed_files = []
    for value in data.values():
        changed_files.extend(value)
    return list(set(f.split('/')[0] for f in changed_files))


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

        # prepare '--exclude' arguments if any for the aws sync command
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
            # current date and time
            time_now = datetime.now().strftime('%d.%m.%Y at %H:%M:%S')

            # try to sync this folder with the same folder in the bucket
            try:
                aws_sync(dir_path, bucket_path, exclude)
                print(f"Synced '{changed_dirs[i]}' on {time_now}.")
            except subprocess.CalledProcessError as e:
                print(f"UNABLE to sync '{changed_dirs[i]}' on {time_now}.")
                print(f"The sync command returned exit status {e.returncode}.")
            except subprocess.TimeoutExpired as e:
                print(f"UNABLE to sync '{changed_dirs[i]}' on {time_now}.")
                print(f"The sync command timed out after {e.timeout} seconds.")

        # save/overwrite the json file with the new index
        save_json(json_index_file, new_index)

        # print divider
        print('-' * 53)

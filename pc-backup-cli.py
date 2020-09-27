#!/usr/bin/env python

import os
import json
from datetime import datetime
from variables import *


ydef aws_sync(dir_path, bucket_name, exclude):
    """ dir_path: the local path to the directory (string).
        bucket_path: the bucket path to the directory (string).
        exclude: --exclude arguments if any (string).
        It constructs and runs an aws sync command.
        Returns: None. """
    sync = '/usr/local/bin/aws s3 sync '
    sync += f'"{dir_path}/" "s3://{bucket_name}/" {exclude}'
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
    # The following 6 variables are defined in a separate variables.py file:
    # 1. dir_path, 2. bucket_name, 3. dirs_to_sync, 4. json_index_file,
    # 5. exclude_prefixes, 6. exclude_suffixes.
    # These variables are sensitive and unique to your environment.
    # Read the README.md file.

    # the directory's current/new index
    new_index = compute_dir_index(
        dir_path, dirs_to_sync, exclude_prefixes, exclude_suffixes)

    # the directory's old index
    old_index = read_json(json_index_file)

    # possible statement to print for logging purposes
    statements_to_print = []

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

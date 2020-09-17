#!/usr/bin/env python

import os
import json
from datetime import datetime
from paths import *


def compute_dir_index(path, ignore=()):
    """ path: path to the directory.
        ignore: tuple with suffixes of filenames to ignore
        Returns a dictionary with 'file: last modified time'. """
    index = {}
    # traverse the dir
    for root, dirs, filenames in os.walk(path):
        # loop through the files in the current directory
        for f in filenames:
            # if the file doesn't have a suffix we want to ignore
            if not f.endswith(ignore):
                # get the file path relative to the dir
                file_path = os.path.relpath(os.path.join(root, f), path)
                # get the last modified time of that file
                mtime = os.path.getmtime(os.path.join(path, file_path))
                # put them in the index
                index[file_path] = mtime

    # return a dict of files as keys and
    # last modified time as their values
    return index


if __name__ == "__main__":
    # construct filenames with lowercase letters and no white spaces
    json_names = [name.replace(' ', '-').lower() + '.json' for name in dirs]

    # build an exclude string if at all for the aws sync command
    exclude = ''
    for strng in ignore:
        exclude += f'--exclude "*{strng}" '
    exclude = exclude.rstrip()

    # possible statement to print for logging purposes
    statements_to_print = []

    for i in range(len(dirs)):
        # the path to the folder
        dir_path = root + dirs[i]

        # compute the new index for this folder
        new_index = compute_dir_index(dir_path, ignore)

        # the old index json file
        json_file = index + json_names[i]

        # try to read the old index json file
        try:
            with open(json_file, 'r') as f:
                old_index = json.load(f)
        # if there's no such file the old_index is an empty dict
        except IOError:
            old_index = {}

        # if there's a difference
        if new_index != old_index:

            # sync this folder with the bucket
            os.system(f'/usr/local/bin/aws s3 sync \
                "{dir_path}/" "{bucket}/{dirs[i]}/" {exclude} \
                --storage-class STANDARD_IA --delete --quiet')

            # current date and time
            time_now = datetime.now().strftime('%d.%m.%Y at %H:%M:%S')
            # append a statement to print later
            statements_to_print.append(f"Synced '{dirs[i]}' on {time_now}.")

            # save/overwrite the json file with the new index
            with open(json_file, 'w') as f:
                json.dump(new_index, f, indent=4)

    if statements_to_print:
        for statement in statements_to_print:
            print(statement)
        print('-----------------------------------------------------')

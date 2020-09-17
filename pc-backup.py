#!/usr/bin/env python

import os
import json
from datetime import datetime
from paths import *


def compute_dir_index(path):
    """ path: path to the directory.
        Returns a dictionary with 'file: last modified time'. """
    index = {}
    # traverse the dir
    for root, dirs, filenames in os.walk(path):
        # loop through the files in the current directory
        for f in filenames:
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

    for i in range(len(dirs)):
        # the path to the folder
        dir_path = root + dirs[i]

        # compute the new index for this folder
        new_index = compute_dir_index(dir_path)

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
            print(f'Syncing {dirs[i]}... ', end='')
            # sync this folder with the bucket
            os.system(f'aws s3 sync "{dir_path}/" "{bucket}/{dirs[i]}/" \
                --storage-class STANDARD_IA --delete --quiet')
            print('Done.')
            # save/overwrite the json file with the new index
            with open(json_file, 'w') as f:
                json.dump(new_index, f, indent=4)
        else:
            print(f'No changes in {dirs[i]}.')

    # current date and time
    time_now = datetime.now().strftime('%d.%m.%Y, %H:%M')
    print(f'Syncing finished at {time_now}.')
    print('--------------------------------------')

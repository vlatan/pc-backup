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

import json
import psutil
from pathlib import Path


# read config file
config_file = json.loads(Path("config.json").read_text())


# get config vars
DIRECTORIES = config_file.get("DIRECTORIES")
BUCKET_NAME = config_file.get("BUCKET_NAME")
STORAGE_CLASS = config_file.get("STORAGE_CLASS")
PREFIXES = tuple(p for p in config_file.get("PREFIXES", []) if p)
SUFFIXES = tuple(s for s in config_file.get("SUFFIXES", []) if s)
MAX_ACTIVE_TASKS = int(config_file.get("MAX_POOL_SIZE", 0)) or psutil.cpu_count()

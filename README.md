# PC Backup

PC Backup is a sort of **DIY Google Drive/Dropbox** solution that synchronizes
specified folders from your PC to an S3 bucket in a specified intervals
via cronjob. It basically computes an index of files along with their
last modified timestamps and if there are any changes from the previous state
it deletes/uploads files from/to the S3 bucket accordingly.


### Prerequisites

- [AWS Account](https://aws.amazon.com/)
- [S3 bucket](https://aws.amazon.com/s3/)
- [IAM user and policy](https://docs.aws.amazon.com/AmazonS3/latest/dev/walkthrough1.html)
for programmatic access to the S3 bucket
- AWS CLI [installed](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) and [configured](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html) on your machine, and
- Means for scheduling a [cronjob](https://crontab.guru/).

Don't forget to [enable versioning](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/enable-versioning.html)
and to [create a lifecycle policy](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-lifecycle.html)
for the noncurrent versions of the objects in your S3 bucket so this setup
can be as close to Google Drive or Dropbox as possible.

Additionally you'll need:
- [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (AWS SDK for Python)
- [psutil](https://pypi.org/project/psutil/) (cross-platform library for retrieving information on running processes and system utilization in Python)
- more in `requirements.txt`


### Usage

Clone the repo and cd into its folder:

```
git clone https://github.com/vlatan/pc-backup.git && cd pc-backup
```

Create virtual environment `.venv`, activate it, upgrade `pip` and install the dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install pip --upgrade
pip install -r requirements.txt
```

Create `config.json` file and define several variables in a JSON document format specific to your needs:

```
{
    "DIRECTORIES": [
        "/home/john/music",
        "/home/john/videos",
        "/home/documents"
    ],
    "BUCKET_NAME": "your-bucket-name",
    "STORAGE_CLASS": "STANDARD_IA",
    "PREFIXES": [
        "__",
        "~",
        "."
    ],
    "SUFFIXES": [
        ".log",
        ".out",
        ".crdownload",
        ".tmp",
        ".part",
        ".partial",
        ".torrent",
        "desktop.ini"
    ],
    "MAX_POOL_SIZE": 50
}
```

`DIRECTORIES` - list of absolute paths of the folders you want to track and upload/sync to AWS bucket.  
`BUCKET_NAME` - the name of your AWS S3 bucket that you already prepared for this job.  
`STORAGE_CLASS` - AWS S3 objects [storage class](https://aws.amazon.com/s3/storage-classes/).  
`PREFIXES` - list of prefixes to exclude files/folders with those prefixes (e.g. hidden files).  
`SUFFIXES` - list of suffixes to exclude files/folders with those suffixes (e.g. (e.g. files with certain extensions).  
`MAX_POOL_SIZE` - the size of concurrent chunks of files to delete/upload.


Schedule a cronjob:

```
# run every minute
*/1 * * * * cd /home/user/john/pc-backup && ./backup.py >> logs/backup.out 2>&1
```

### License

[MIT](https://github.com/vlatan/pc-backup/blob/master/LICENSE)



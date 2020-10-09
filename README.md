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
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
installed and configured on your machine, and
- Means for scheduling a [cronjob](https://crontab.guru/).

Don't forget to [enable versioning](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/enable-versioning.html) 
and to [create a lifecycle policy](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-lifecycle.html) 
for the noncurrent versions of the objects in your S3 bucket so this setup 
can be as close to Google Drive or Dropbox as possible.

Additionally you'll need:  
- [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (AWS SDK for Python), and  
- [psutil](https://pypi.org/project/psutil/) (cross-platform library for retrieving information on running processes and system 
utilization in Python)

### Usage

In a separate `constants.py` file define several constants specific to your environment:

```
# path to the user's directory
USER_DIR = '/home/user/john'

# your s3 bucket name
BUCKET_NAME = 'your-bucket-name'

# the names of the directories you want to track and sync in the user's home directory
DIRS = ['music', 'videos', 'documents', 'etc']

# json index file location
INDEX_FILE = f'{USER_DIR}/pc-backup/logs/index.json'

# prefixes to exclude (e.g. hidden files)
PREFIXES = ('__', '~', '.')

# suffixes to exclude (e.g. files with certain extensions)
SUFFIXES = ('.log', '.out', '.crdownload', '.tmp', '.part', '.partial', 'desktop.ini')
```

Schedule a cronjob:

```
# run every minute
*/1 * * * * cd /home/user/john/pc-backup && ./pc-backup-sdk.py >> logs/pc-backup.out 2>&1
```

### License

[MIT](https://github.com/vlatan/pc-backup/blob/master/LICENSE)



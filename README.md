# PC Backup

PC Backup is a sort of **DIY Google Drive/Dropbox** that synchronizes 
specified folders from your PC to an S3 bucket in a specified intervals 
via cronjob. It basically computes an index of files along with their 
timestamps and if there are any changes from the previous state 
it deletes/uploads files from/to the S3 bucket accordingly. It can do 
this in two ways: via **AWS CLI** or **AWS SDK**.

### Prerequisites

- [AWS Account](https://aws.amazon.com/)   
- [S3 bucket](https://aws.amazon.com/s3/)
- [IAM user and policy](https://docs.aws.amazon.com/AmazonS3/latest/dev/walkthrough1.html) 
for programmatic access to the S3 bucket
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
installed and configured on your machine, and
- Means for scheduling a [cronjob](https://crontab.guru/).

Don't forget to [enable versioning](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/enable-versioning.html) 
and [create a lifecycle policy](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-lifecycle.html) 
for the noncurrent versions of the objects in your S3 bucket so this setup 
can be as close to Google Drive or Dropbox as possible.

### Usage

In a separate `variables.py` file define several variables specific for your environment:

```
# path to the user's directory
user_root = '/home/user/john'

# your s3 bucket name
bucket_name = 'your-bucket-name'

# the names of the directories you want to track and sync in the user's home directory
dirs_to_sync = ['music', 'videos', 'documents', 'etc']

# json index file location
json_index_file = f'{user_root}/pc-backup/logs/index.json'

# exclude prefixes (e.g. hidden files)
exclude_prefixes = ('__', '~', '.')

# exclude suffixes (e.g. files with certain extensions)
exclude_suffixes = ('.out', .crdownload', '.part', '.partial', 'desktop.ini')
```

Schedule a cronjob:

```
# run every minute
*/1 * * * * cd /home/user/john/pc-backup && ./pc-backup-sdk.py >> logs/pc-backup.out 2>&1
```

### License

[MIT](https://github.com/vlatan/pc-backup/blob/master/LICENSE)



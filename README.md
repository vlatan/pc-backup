# PC Backup

PC Backup is a sort of **DIY Google Drive/Dropbox** that synchronizes 
specified folders from your PC to an S3 bucket in a specified intervals 
via cronjob. It basically computes an index of files along with their 
timestaps for each folder you want to backup/sync and if there are any 
changes an `aws s3 sync` command is issued.

### Prerequisites

- [AWS Account](https://aws.amazon.com/)   
- [S3 bucket](https://aws.amazon.com/s3/)
- [IAM user and policy](https://docs.aws.amazon.com/AmazonS3/latest/dev/walkthrough1.html) 
for programmatic access to the S3 bucket
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
installed on your machine, and
- Means for scheduling a [cronjob](https://crontab.guru/).

Don't forget to [enable versioning](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/enable-versioning.html) 
and [create a lifecycle policy](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-lifecycle.html) 
for the noncurrent versions of the objects in your S3 bucket so this setup 
can be as close to Google Drive or Dropbox as possible.

### Usage

In a separate `paths.py` file define several variables specific for your environment:

```
# root path to the user's directory
root = '/home/user/john/'

# your s3 bucket path
bucket = 's3://your-bucket/'

# directories you want to sync within the user's directory
dirs = ['music', 'videos', 'documents', 'etc']

# path to the directory where intend to store json index files
index = '/home/user/john/pc-backup/index/'

# exclude prefixes (e.g. hidden files)
exclude_prefixes = ('__', '~', '.')

# exclude suffixes (e.g. files with certain extensions)
exclude_suffixes = ('.out', 'desktop.ini')
```

Schedule a cronjob:

```
# run every minute
*/1 * * * * cd /home/user/john/pc-backup && /usr/bin/python3.6 pc-backup.py >> pc-backup.out
```

### License

[MIT](https://github.com/vlatan/pc-backup/blob/master/LICENSE)



# PC Backup

PC Backup is a sort of DIY Google Drive/Dropbox that backs up specified folders from your PC 
to an S3 bucket in a specified intervals via cronjob. It basically computes 
an index of files along with their timestaps for each folder you want to backup/sync 
and if there are any changes an `aws s3 sync` command is issued.

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

### Instructions

In a separate `paths.py` file define several variables specific for your environment:

```
# your s3 bucket
bucket = 's3://your-bucket'

# root path to the user
root = '/home/user/john/'

# directories to sync within the root
dirs = ['music', 'videos', 'documents']

# path to the directory where you'll store json indexes
index = '/home/user/john/pc-backup/index/'

# list of filenames/extensions to ignore when syncing
ignore = ('.out')
```

Schedule a cronjob:

```
# run every 5 minutes
*/5 * * * * cd /home/user/john/pc-backup && /usr/bin/python3.6 pc-backup.py >> pc-backup.out
```

### License

[MIT](https://github.com/vlatan/pc-backup/blob/master/LICENSE)



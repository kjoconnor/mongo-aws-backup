mongo-aws-backup
================

@gooeyblob / kevino@digg.com

Python script to discover and backup MongoDB replica sets hosted on EC2.

This requires that you are running MongoDB on EC2 on Linux and are using EC2 tags to help manage your MongoDB replica sets.  For instance, we use tags like 'replicaset' with values that represent what replica set they are a part of, but they can really be any EC2 API compatible filter.  Wherever you run this script from will need access to Mongo (typically port 27017) and SSH access, and have your SSH keyfile available to it.

Usage
=====

Put your AWS access key and secret up top, or leave blank if you'd like to use IAM.  You should also put your region up there while you're at it.

```
usage: mongo_backup.py [-h] -r REPLICASET --ec2-filter EC2FILTER -k SSHKEYFILE
                       -u SSHUSER [-n]

optional arguments:
  -h, --help            show this help message and exit
  -r REPLICASET, --replicaset REPLICASET
                        Replica set to back up.
  --ec2-filter EC2FILTER
                        EC2 API compatible filter with which to find
                        instances, ex. 'tag:replicaset,importantthings' will find
                        instances with the tag 'replicaset' and a value of
                        'importantthings'. Multiple filters can be separated by with
                        a semicolon.
  -k SSHKEYFILE, --ssh-keyfile SSHKEYFILE
                        Path to SSH key file to use.
  -u SSHUSER, --user SSHUSER
                        Username to use to connect over SSH.
  -n, --dry-run         Don't actually do anything, just print what we would
                        have done
```
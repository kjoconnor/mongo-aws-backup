mongo-aws-backup
================

@gooeyblob / kevino@digg.com

Python script to discover and back up MongoDB replica sets hosted on EC2.

This requires that you are running MongoDB on EC2 on Linux and are using some type of EC2 filters to help manage your MongoDB replica sets.  For instance, we use tags like 'replicaset' with values that represent what replica set they are a part of, but they can really be any EC2 API compatible filter.  Wherever you run this script from will need access to Mongo (typically port 27017) and SSH access, and have your SSH keyfile available to it.

You should also be writing data to a separate EBS volume.

Please be careful with this!  Run in dry run first, and maybe try some of the commands out on their own first to make sure this won't disrupt your replica sets.

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
                        'importantthings'. Multiple filters can be separated with
                        a semicolon.
  -k SSHKEYFILE, --ssh-keyfile SSHKEYFILE
                        Path to SSH key file to use.
  -u SSHUSER, --user SSHUSER
                        Username to use to connect over SSH.
  -n, --dry-run         Don't actually do anything, just print what we would
                        have done
```

Once started, it'll try and discover the applicable EC2 instances, connect to their mongo instances, then run the following tests on the replica set:

- Ensure there is a member available to back up from without affecting the set's availability
- Ensure all members are in a good state
- Ensure all members are healthy
- Ensure there is not excessive replication lag

It will then choose a member to back up from, log in over SSH, detect what file system mongo is writing to, freeze mongo and flush writes, and create a snapshot at EC2 and tag it with some metadata. Finally, it unlocks the backup target and puts it back into normal use.

Roadmap
=======
- Rotation
 - You can just use the bundled `delete_snapshots.py` to delete old snapshots that match certain filters, but ideally this would employ some sort of backup rotation strategy for you.
- Use replSetMaintenance
 - I don't have any good set up to test this on, but this would be preferable than the other strange dance we do currently to make sure the replica set is in a consistent state.
- Configuration file (and merge with command line switches at runtime)
- Version checking and different behaviors
- Other discovery methods
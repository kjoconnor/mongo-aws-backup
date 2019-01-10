#!/usr/bin/env python
"""
NAME
    mongo_backup.py - Backup all MongoDB databases on a replica set
DESCRIPTION
    fixme
EXAMPLES
    ./mongo_backup.py -r MyReplicaSet --ec2-filter 'tag:mongodb,MyReplicaSet'

TODO
    - Check if `mongodump` is installed before anything
    - Verify `mongodump` command exit code
    - Revisit params
    - Better logging messages
    - Document restore process
"""

import argparse
import logging
import subprocess
import tarfile

from boto.ec2 import connect_to_region as ec2_connect_to_region
from boto.s3 import connect_to_region as s3_connect_to_region, key
from datetime import datetime
from os import listdir
from pymongo import MongoClient

# Leave these blank to just use IAM roles
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = 'us-west-2'
# S3 settings
s3_bucket_name = '2tor-backups'
s3_bucket_dest_dir = 'MongoDB_backups_us-east-1'
s3_bucket_region = 'us-east-1'


class AwsMongoBackup(object):

    def __init__(self,
                 replicaset=None,
                 filters=None,
                 dryrun=False,
                 region=None,
                 logger=None):
        self.creation_time = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

        if logger is not None:
            self.logger = logger

        if region is None:
            region = AWS_REGION

        if AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY:
            self.ec2 = ec2_connect_to_region(
                region,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            self.s3 = s3_connect_to_region(
                s3_bucket_region,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        else:
            self.ec2 = ec2_connect_to_region(
                region
            )
            self.s3 = s3_connect_to_region(
               s3_bucket_region
            )

        if replicaset is None:
            raise RuntimeError('Replicaset must be provided.')

        self.replicaset = replicaset

        if filters:
            self.instances = self._instances_via_filters(filters=filters)
        else:
            raise RuntimeError('An API filter must be provided.')
        self.logger.debug("Found instances %s" % self.instances)

        self.mongo = self._mongo(instances=self.instances)
        self.logger.debug("Connected to mongo %s" % self.mongo)

        self.dryrun = dryrun

    def _instances_via_filters(self, filters=None):
        if filters is None:
            raise ValueError('Filters must be a dict of valid EC2 API filters')

        reservations = self.ec2.get_all_instances(
            filters=filters
        )

        instances = []

        for reservation in reservations:
            instances.extend(reservation.instances)

        return instances

    def _mongo(self, instances, force=False):
        if not hasattr(AwsMongoBackup, 'mongo') or force:
            mongo_rs_str = ','.join([x.private_dns_name for x in instances])
            self.logger.debug("Connecting to mongo URI %s" % mongo_rs_str)
            self.mongo = MongoClient(
                mongo_rs_str,
                replicaSet=self.replicaset
            )

            if self.mongo.admin.command('ping'):
                return self.mongo
            else:
                self.mongo = None
                return None

    def test_replicaset(self):
        test_result = True
        err_str = ''

        optime_dates = []
        rs_states = {}
        hidden_members = []
        secondaries = []

        rs_status = self.mongo.admin.command('replSetGetStatus')
        rs_member_hosts = [z[0] for z in self.mongo.nodes]

        for rs_member in rs_status['members']:
            if rs_member['name'].split(':')[0] not in rs_member_hosts and \
               rs_member['stateStr'] != 'ARBITER':
                hidden_members.append((
                    rs_member['name'].split(':')[0],
                    int(rs_member['name'].split(':')[1])
                ))
            elif rs_member['stateStr'] == 'SECONDARY':
                secondaries.append((
                    rs_member['name'].split(':')[0],
                    int(rs_member['name'].split(':')[1])
                ))
            try:
                rs_states[rs_member['state']] += 1
            except KeyError:
                rs_states[rs_member['state']] = 1

            if rs_member['state'] not in [1, 2, 7]:
                # primary, secondary, arbiter
                err_str = "RS member {rs_member} has a state of {state}, "\
                    "please check RS integrity and try again.".format(
                        rs_member=rs_member['name'],
                        state=rs_member['stateStr']
                    )
                test_result = False
                return (test_result, err_str)
            self.logger.debug("Member %s passed state" % rs_member['name'])

            if rs_member.get('health', 1) != 1:
                err_str = "RS member {rs_member} is marked as unhealthy, "\
                    "please check RS integrity and try again.".format(
                        rs_member=rs_member['name']
                    )
                test_result = False
                return (test_result, err_str)
            self.logger.debug("Member %s passed health" % rs_member['name'])

            # Arbiters don't have optimeDate and pingMs, skip checks on marbs
            if rs_member['stateStr'] != 'ARBITER':
                pingMs = rs_member.get('pingMs', 0)
                if pingMs > 15:
                    err_str = "Ping time for RS member {rs_member} is "\
                        "{pingMs}. Please check network connectivity and try "\
                        "again.".format(
                            rs_member=rs_member['name'],
                            pingMs=pingMs
                         )
                    test_result = False
                    return (test_result, err_str)
                self.logger.debug("Member %s passed pingMs"
                                  % rs_member['name'])

                optime_dates.append(rs_member['optimeDate'])

        self.hidden_members = hidden_members
        self.secondaries = secondaries

        replication_lag = (max(optime_dates) -
                           min(optime_dates)).total_seconds()
        if replication_lag > 59:
            err_str = "There's a {replication_lag} seconds replication lag, "\
                "too much to continue.".format(replication_lag=replication_lag)
            test_result = False
            return (test_result, err_str)
        self.logger.debug("Passed replication lag test: {replication_lag} "
                          "seconds".format(replication_lag=replication_lag))

        if len(secondaries) + len(hidden_members) < 1:
            err_str = "There needs to be at least one secondary or a hidden"\
                " member available to do backups. Please check RS integrity "\
                "and try again."
            test_result = False
            return (test_result, err_str)

        self.logger.debug("Mongo secondaries test passed")

        if rs_states[1] != 1:
            err_str = "There needs to be one and exactly one mongo primary to"\
                " do backups. Please check RS integrity and try again."
            test_result = False
            return (test_result, err_str)
        self.logger.debug("Passed primary mongo test")

        return (test_result, err_str)

    def choose_member(self):
        if self.hidden_members:
            return self.hidden_members.pop()
        else:
            return self.secondaries.pop()

    def backup(self):
        # Test that the replica set is in a good state to perform backups
        test_result, err_str = self.test_replicaset()
        if test_result is False:
            raise RuntimeError(err_str)

        # Choose a member from which to back up
        backup_member = self.choose_member()

        # Connect to the backup target directly
        backup_member_mongo = MongoClient(
            host=backup_member[0],
            port=backup_member[1]
        )
        self.logger.debug("Connected to mongo target %s" % backup_member_mongo)

        freeze_rs = True
        if backup_member_mongo.admin.command('isMaster').get('hidden', False):
            # This member is hidden so we can safely take backups without
            # doing any other maintenance work
            freeze_rs = False

        # Remove the member from the replica set (mark as hidden)
        if freeze_rs:
            # Can probably use replSetMaintenance here but not available
            # in my testing version
            if self.dryrun:
                self.logger.debug("Would have frozen replica set")
            else:
                self.logger.debug('Freezing replica set')
                backup_member_mongo.admin.command({'replSetFreeze': 86400})

        else:
            self.logger.debug(
                "Skipping replica set freeze, %s is a hidden member"
                % backup_member[0]
            )

        if self.dryrun:
            self.logger.debug(
                "Would have dumped databases on {backup_member}"
                .format(backup_member=backup_member)
            )
        else:
            self.logger.debug(
                "Dumping databases on {backup_member}"
                .format(backup_member=backup_member)
            )
            for database in backup_member_mongo.database_names():
                if database not in args.exclude_dbs:
                    mongodump = 'mongodump -h {backup_member} -d {database} '\
                                '-o {backup_member} --quiet'.format(
                                    backup_member=backup_member[0],
                                    database=database
                                )
                    mongodump = mongodump.split(' ')
                    subprocess.check_output(mongodump,
                                            stderr=subprocess.STDOUT)

        # Unlock mongo
        if self.dryrun:
            self.logger.debug("Would have unlocked mongo")
        else:
            if freeze_rs:
                self.logger.debug('Unfreezing replica set')
                backup_member_mongo.admin.command({'replSetFreeze': 0})

        # Archive and upload to S3
        if self.dryrun:
            self.logger.debug("Would have archived dumps and uploaded to S3")
        else:
            self.logger.debug("Archiving dumps and uploading to S3")
            dumps = listdir(backup_member[0])
            for dump in dumps:
                archive_path = backup_member[0] + '/' + dump
                archive_name = dump + '_' + self.creation_time + '.tar.gz'
                with tarfile.open(archive_name, 'w:gz') as tar:
                    tar.add(archive_path, arcname=dump)
                bucket = self.s3.get_bucket(s3_bucket_name)
                key_obj = key.Key(bucket)
                key_obj.key = s3_bucket_dest_dir + '/' + archive_name
                key_obj.set_contents_from_filename(archive_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-r",
        "--replicaset",
        help="Replica set to back up.",
        dest="replicaset",
        required=True
    )
    parser.add_argument(
        "--ec2-filter",
        help="EC2 API compatible filter with which to find instances, ex. "
             "'tag:replicaset,importantthings' will find instances with the "
             "tag 'replicaset' and a value of 'importantthings'. Multiple "
             "filters can be separated with a semicolon.",
        dest="ec2filter",
        required=True
    )
    parser.add_argument(
        "--exclude-dbs",
        help="Names of databases to exclude when dumping, e.g. 'local'. "
             "Separate multiple values by space, e.g. 'admin local'",
        dest="exclude_dbs",
        nargs='*',
        default=[],
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Don't actually do anything, just print what we would have done.",
        dest="dryrun"
    )
    args = parser.parse_args()

    logger = logging.getLogger('mongobackup')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    ch.setFormatter(formatter)
    logger.addHandler(ch)

    ec2filter = {}

    if ',' not in args.ec2filter:
        raise RuntimeError("EC2 filter provided is invalid")

    for x in args.ec2filter.split(';'):
        ec2filter.update(
            {x.split(',')[0]: x.split(',')[1]}
        )

    mb = AwsMongoBackup(
        replicaset=args.replicaset,
        filters=ec2filter,
        dryrun=args.dryrun,
        logger=logger
    )

    mb.backup()

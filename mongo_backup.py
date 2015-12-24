#!/usr/bin/env python
"""
NAME
    fixme
DESCRIPTION
    fixme
EXAMPLES
    ./mongo_backup.py -r MyReplicaSet --ec2-filter 'tag:mongodb,MyReplicaSet'

TODO
    Add rotation?
    Configging out more stuff
    Version checks
    Finish or remove _instances_via_ids, seems like just using filters is
        better though

    Restore process:
        fixme
"""

import argparse
import logging

from boto.ec2 import connect_to_region as ec2_connect_to_region
from datetime import datetime
from pymongo import MongoClient

# Leave these blank to just use IAM roles
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = 'us-west-2'

class AwsMongoBackup(object):

    def __init__(self,
                 replicaset=None,
                 filters=None,
                 instance_ids=None,
                 dryrun=False,
                 region=None,
                 logger=None):
        self.creation_time = datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S")

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
        else:
            self.ec2 = ec2_connect_to_region(
                region
            )

        if replicaset is None:
            raise RuntimeError('replicaset must be provided.')

        self.replicaset = replicaset

        if filters:
            self.instances = self._instances_via_filters(filters=filters)
        elif instance_ids:
            self.instances = self._instances_via_ids(instance_ids=instance_ids)
        else:
            raise RuntimeError('Either an API filter or a list of instance'
                               'IDs must be provided.')
        self.logger.debug("found instances %s" % self.instances)

        self.mongo = self._mongo(instances=self.instances)
        self.logger.debug("connected to mongo %s" % self.mongo)

        self.dryrun = dryrun

    def _instances_via_filters(self, filters=None):
        if filters is None:
            raise ValueError('filters must be a dict of valid EC2 API filters')

        reservations = self.ec2.get_all_instances(
            filters=filters
        )

        instances = []

        for reservation in reservations:
            instances.extend(reservation.instances)

        return instances

    def _instances_via_ids(self, instance_ids=None):
        if instance_ids is None or type(instance_ids) is not 'list':
            raise ValueError('instances must be provided in a list')

        raise NotImplementedError("I'll come back to this later.")

    def _mongo(self, instances, force=False):
        if not hasattr(AwsMongoBackup, 'mongo') or force:
            mongo_rs_str = ','.join([x.public_dns_name for x in instances])
            self.logger.debug("connecting to mongo URI %s" % mongo_rs_str)
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
            if rs_member['name'].split(':')[0] not in rs_member_hosts and rs_member['stateStr'] != 'ARBITER':
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
                    "please check RS integrity and try again."\
                    .format(
                        rs_member=rs_member['name'],
                        state=rs_member['stateStr']
                    )
                test_result = False
                return (test_result, err_str)
            self.logger.debug("member %s passed state" % rs_member['name'])

            if rs_member.get('health', 1) != 1:
                err_str = "RS member {rs_member} is marked as unhealthy, "\
                    "please check RS integrity and try again."\
                    .format(rs_member=rs_member['name'])
                test_result = False
                return (test_result, err_str)
            self.logger.debug("member %s passed health" % rs_member['name'])

            # Arbiters don't have optimeDate and pingMs, skip checks on marbs
            if rs_member['stateStr'] != 'ARBITER':
                if rs_member.get('pingMs', 0) > 10:
                    err_str = "ping time for RS member {rs_member} is larger than"\
                        "10ms.  Please check network connectivity and try again."\
                        .format(rs_member=rs_member['name'])
                    test_result = False
                    return (test_result, err_str)
                self.logger.debug("member %s passed pingMs" % rs_member['name'])

                optime_dates.append(rs_member['optimeDate'])

        self.hidden_members = hidden_members
        self.secondaries = secondaries

        if (max(optime_dates) - min(optime_dates)).total_seconds() > 5:
            err_str = "optimeDates is over 5 seconds, there is too much "\
                "replication lag to continue."
            test_result = False
            return (test_result, err_str)
        self.logger.debug("passed replication lag test")

        if len(secondaries) + len(hidden_members) < 1:
            err_str = "There needs to be at least one secondary or a hidden"\
                " member available to do backups.  Please check RS integrity "\
                "and try again."
            test_result = False
            return (test_result, err_str)

        self.logger.debug("mongo secondaries test passed")

        if rs_states[1] != 1:
            err_str = "There needs to be one and exactly one mongo primary to"\
                " do backups.  Please check RS integrity and try again."
            test_result = False
            return (test_result, err_str)
        self.logger.debug("passed primary mongo test")

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
        self.logger.debug("connected to mongo target %s" % backup_member_mongo)

        freeze_rs = True
        if backup_member_mongo.admin.command('isMaster').get('hidden', False):
            # This member is hidden so we can safely take backups without
            # doing any other maintenance work
            freeze_rs = False

        # Remove the member from the replicaset (mark as hidden)
        if freeze_rs:
            # Can probably use replSetMaintenance here but not available
            # in my testing version
            if self.dryrun:
                self.logger.debug("Would have frozen replicaset")
            else:
                self.logger.debug('Freezing replicaset')
                backup_member_mongo.admin.command({'replSetFreeze': 86400})

        else:
            self.logger.debug(
                "skipping replicaset freeze, %s is a hidden member"
                % backup_member[0]
            )

        # Fsynclock mongo
        if self.dryrun:
            self.logger.debug(
                "Would have fsynclocked {backup_member}"
                .format(backup_member=backup_member)
            )
        else:
            self.logger.debug(
                "fsync/locking {backup_member}"
                .format(backup_member=backup_member)
            )
            backup_member_mongo.fsync(lock=True)

        if self.dryrun:
            self.logger.debug(
                "Would have created mongodump of {backup_member}"
                .format(backup_member=backup_member)
            )
        else:
            # `mongodump` goes here
            print

        # Unlock mongo
        if self.dryrun:
            self.logger.debug("Would have unlocked mongo")
        else:
            if freeze_rs:
                self.logger.debug('unfreezing replicaset')
                backup_member_mongo.admin.command({'replSetFreeze': 0})

            self.logger.debug(
                "unlocking {backup_member}"
                .format(backup_member=backup_member)
            )
            backup_member_mongo.unlock()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-r",
        "--replicaset",
        action="store",
        help="Replica set to back up.",
        dest="replicaset",
        required=True
    )
    parser.add_argument(
        "--ec2-filter",
        action="store",
        help="EC2 API compatible filter with which to find instances, ex. "
             "'tag:replicaset,importantthings' will find instances with the "
             "tag 'replicaset' and a value of 'importantthings'.  Multiple "
             "filters can be separated with a semicolon.",
        dest="ec2filter",
        required=True
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Don't actually do anything, just print what we would have done",
        dest="dryrun"
    )
    args = parser.parse_args()

    logger = logging.getLogger('mongobackup')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    ch.setFormatter(formatter)
    logger.addHandler(ch)

    ec2filter = {}

    if ',' not in args.ec2filter:
        raise RuntimeError("ec2filter provided is invalid")

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

import argparse
import logging

from boto.ec2 import connect_to_region as ec2_connect_to_region
from datetime import datetime
from paramiko import SSHClient, AutoAddPolicy
from pymongo import MongoReplicaSetClient, MongoClient

# Leave these blank to just use IAM roles
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = 'us-east-1'

# TODO:
# Add rotation?
# Configging out more stuff
# Version checks
# Finish or remove _instances_via_ids, seems like just using filters is
#   better though

# Command line:
# python mongo_backup.py -r MyReplicaSet \
#   --ec2-filter "tag:mongodb,MyReplicaSet" -k key.pem -u ubuntu

# Restore process:
# Create volume from snapshot
# Attach to instance
# Mount (and configure mongo dbpath if necessary)
# chown mounted path to mongo:mongo


class AwsMongoBackup(object):

    def __init__(self,
                 replicaset=None,
                 filters=None,
                 instance_ids=None,
                 ssh_opts=None,
                 dryrun=False,
                 region=None,
                 logger=None):
        self.creation_time = datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S")

        if logger is not None:
            self.logger = logger

        if region is None:
            region = 'us-east-1'

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

        self.ssh_opts = ssh_opts
        self.logger.debug("set ssh opts to %s" % self.ssh_opts)

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
            self.mongo = MongoReplicaSetClient(
                mongo_rs_str,
                replicaSet=self.replicaset
            )

            if self.mongo.alive():
                return self.mongo
            else:
                self.mongo = None
                return None

    def _ssh(self, hostname, ssh_opts):
        self.ssh = SSHClient()
        self.ssh.set_missing_host_key_policy(AutoAddPolicy())
        self.ssh.connect(hostname=hostname, **ssh_opts)

        self.logger.debug("connected via ssh to %s" % hostname)

        return self.ssh

    def test_replicaset(self):
        test_result = True
        err_str = ''

        optime_dates = []
        rs_states = {}
        hidden_members = []

        rs_status = self.mongo.admin.command('replSetGetStatus')
        rs_member_hosts = [z[0] for z in self.mongo.hosts]

        for rs_member in rs_status['members']:
            if rs_member['name'].split(':')[0] not in rs_member_hosts:
                hidden_members.append((
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

            if rs_member.get('pingMs', 0) > 10:
                err_str = "ping time for RS member {rs_member} is larger than"\
                    "10ms.  Please check network connectivity and try again."\
                    .format(rs_member=rs_member['name'])
                test_result = False
                return (test_result, err_str)
            self.logger.debug("member %s passed pingMs" % rs_member['name'])

            optime_dates.append(rs_member['optimeDate'])

        self.hidden_members = hidden_members

        if (max(optime_dates) - min(optime_dates)).total_seconds() > 5:
            err_str = "optimeDates is over 5 seconds, there is too much "\
                "replication lag to continue."
            test_result = False
            return (test_result, err_str)
        self.logger.debug("passed replication lag test")

        if len(self.mongo.secondaries) + len(hidden_members) < 2:
            err_str = "There needs to be at least two secondaries or a hidden"\
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
            return self.mongo.secondaries.pop()

    def backup(self):
        # Test that the replica set is in a good state to perform backups
        test_result, err_str = self.test_replicaset()
        if test_result is False:
            raise RuntimeError(err_str)

        # Choose a member from which to back up
        backup_member = self.choose_member()

        self._ssh(backup_member[0], self.ssh_opts)

        # Get the instance ID
        stdin, stdout, stderr = self.ssh.exec_command(
            '/usr/bin/curl http://169.254.169.254/latest/meta-data/instance-id'
        )

        instance_id = stdout.readline().rstrip()
        self.logger.debug("Working on instance %s" % instance_id)

        reservation = self.ec2.get_all_instances(instance_ids=[instance_id, ])
        instance = reservation[0].instances[0]

        self.logger.debug("got boto ec2 instance %s" % instance)

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

        # Find what volume database data is on
        cfg = backup_member_mongo.admin.command('getCmdLineOpts')

        cfg_data_volume = cfg['parsed']['dbpath']

        self.logger.debug("found parsed dbpath of %s" % cfg_data_volume)

        stdin, stdout, stderr = self.ssh.exec_command(
            '/usr/bin/sudo /bin/df {cfg_data_volume} | '
            '/bin/grep -v "Filesystem"'
            .format(cfg_data_volume=cfg_data_volume)
        )

        mount_info = stdout.readline().rstrip()
        mount_info = mount_info.split(' ')[0]

        self.logger.debug("working on mount %s" % mount_info)

        # Find the matching EBS volume for this mount point
        volumes = self.ec2.get_all_volumes(
            filters={'attachment.instance-id': instance_id}
        )

        data_volume = None
        for volume in volumes:
            # There's a strange thing that happens, /dev/sdh1 can magically
            # become /dev/xdh1 at boot time on instances.  Check for both.
            volume_mount_point = volume.attach_data.device
            if volume_mount_point == mount_info or \
                    volume_mount_point.replace('sd', 'xvd') == mount_info:
                data_volume = volume

        if data_volume is None:
            raise RuntimeError("Couldn't find EBS data volume!")

        self.logger.debug("found data volume %s" % data_volume)

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
                "Would have created snapshot of volume {volume}"
                .format(volume=data_volume)
            )
            self.current_snapshot = None
        else:
            self.logger.debug("creating snapshot of %s" % data_volume)
            snapshot = data_volume.create_snapshot(
                description="mongobackup {date} {replicaset}".format(
                    date=self.creation_time,
                    replicaset=self.replicaset)
            )

            self.current_snapshot = snapshot.id

            tags = {
                'replicaset': self.replicaset,
                'sourcehost': backup_member[0],
                'creation_time': self.creation_time
            }
            self.logger.debug(
                "adding tags %s to snapshot %s" % (tags, snapshot)
            )

            self.ec2.create_tags(
                resource_ids=[snapshot.id, ],
                tags=tags
            )

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
        "-k",
        "--ssh-keyfile",
        action="store",
        help="Path to SSH key file to use.",
        dest="sshkeyfile",
        required=True
    )
    parser.add_argument(
        "-u",
        "--user",
        action="store",
        help="Username to use to connect over SSH.",
        dest="sshuser",
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
        ssh_opts={
            'key_filename': args.sshkeyfile,
            'username': args.sshuser
        },
        dryrun=args.dryrun,
        logger=logger
    )

    mb.backup()

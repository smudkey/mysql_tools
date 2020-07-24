#!/usr/bin/python
import argparse
import copy
import pprint
import simplejson

from lib import host_utils
from lib import mysql_lib
from lib import environment_specific

log = environment_specific.setup_logging_defaults(__name__)
chat_handler = environment_specific.BufferingChatHandler()
log.addHandler(chat_handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action',
                        help=("What modification to make. If 'auto', the host "
                              "replacement log will be used to determine what "
                              "what role to use. Default is auto."),
                        choices=['add_subordinate', 'add_dr_subordinate',
                                 'swap_main_and_subordinate',
                                 'swap_subordinate_and_dr_subordinate'],
                        default='auto')
    parser.add_argument('instance',
                        help='What instance to act upon')
    parser.add_argument('--dry_run',
                        help=('Do not actually modify zk, just show '
                              'what would be modify'),
                        default=False,
                        action='store_true')
    parser.add_argument('--dangerous',
                        help=('If you need to swap_main_and_subordinate in zk'
                              'outside of the failover script, that is '
                              'dangerous and you will need this flag.'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    action = args.action
    instance = host_utils.HostAddr(args.instance)

    if args.dry_run:
        log.removeHandler(chat_handler)

    if action == 'add_subordinate':
        add_replica_to_zk(instance, host_utils.REPLICA_ROLE_SLAVE,
                          args.dry_run)
    elif action == 'add_dr_subordinate':
        add_replica_to_zk(instance, host_utils.REPLICA_ROLE_DR_SLAVE,
                          args.dry_run)
    elif action == 'swap_main_and_subordinate':
        if args.dangerous:
            swap_main_and_subordinate(instance, args.dry_run)
        else:
            raise Exception('To swap_main_and_subordinate in zk outside of the '
                            'failover script is very dangerous and the '
                            '--dangerous flag was not supplied.')
    elif action == 'swap_subordinate_and_dr_subordinate':
        swap_subordinate_and_dr_subordinate(instance, args.dry_run)
    else:
        raise Exception('Invalid action: {action}'.format(action=action))


def auto_add_instance_to_zk(port, dry_run):
    """ Try to do right thing in adding a server to zk

    Args:
    port - The port of replacement instance on localhost
    dry_run - If set, do not modify zk
    """
    instance = host_utils.HostAddr(':'.join([host_utils.HOSTNAME, str(port)]))
    try:
        conn = mysql_lib.get_mysqlops_connections()
        log.info('Determining replacement for port {}'.format(port))
        instance_id = host_utils.get_local_instance_id()
        role = determine_replacement_role(conn, instance_id)
        log.info('Adding server as role: {role}'.format(role=role))
    except Exception, e:
        log.exception(e)
        raise
    add_replica_to_zk(instance, role, dry_run)

    if not dry_run:
        log.info('Updating host_replacement_log')
        update_host_replacement_log(conn, instance_id)


def determine_replacement_role(conn, instance_id):
    """ Try to determine the role an instance should be placed into

    Args:
    conn - A connection to the reporting server
    instance - The replacement instance

    Returns:
    The replication role which should be either 'subordinate' or 'dr_subordinate'
    """
    zk = host_utils.MysqlZookeeper()
    cursor = conn.cursor()
    sql = ("SELECT old_host "
           "FROM mysqlops.host_replacement_log "
           "WHERE new_instance = %(new_instance)s ")
    params = {'new_instance': instance_id}
    cursor.execute(sql, params)
    log.info(cursor._executed)
    result = cursor.fetchone()
    if result is None:
        raise Exception('Could not determine replacement host')

    old_host = host_utils.HostAddr(result['old_host'])
    log.info('Host to be replaced is {old_host}'
             ''.format(old_host=old_host.hostname))

    repl_type = zk.get_replica_type_from_instance(old_host)

    if repl_type == host_utils.REPLICA_ROLE_MASTER:
        raise Exception('Corwardly refusing to replace a main!')
    elif repl_type is None:
        raise Exception('Could not determine replacement role')
    else:
        return repl_type


def get_zk_node_for_replica_set(kazoo_client, replica_set):
    """ Figure out what node holds the configuration of a replica set

    Args:
    kazoo_client - A kazoo_client
    replica_set - A name for a replica set

    Returns:
    zk_node - The node that holds the replica set
    parsed_data - The deserialized data from json in the node
    """
    for zk_node in [environment_specific.DS_ZK, environment_specific.GEN_ZK]:
        znode_data, meta = kazoo_client.get(zk_node)
        parsed_data = simplejson.loads(znode_data)
        if replica_set in parsed_data:
            return (zk_node, parsed_data, meta.version)
    raise Exception('Could not find replica_set {replica_set} '
                    'in zk_nodes'.format(replica_set=replica_set))


def remove_auth(zk_record):
    """ Remove passwords from zk records

    Args:
    zk_record - A dict which may or not have a passwd or userfield.

    Returns:
    A dict which if a passwd or user field is present will have the
    values redacted
    """
    ret = copy.deepcopy(zk_record)
    if 'passwd' in ret:
        ret['passwd'] = 'REDACTED'

    if 'user' in ret:
        ret['user'] = 'REDACTED'

    return ret


def add_replica_to_zk(instance, replica_type, dry_run):
    """ Add a replica to zk

    Args:
    instance - A hostaddr object of the replica to add to zk
    replica_type - Either 'subordinate' or 'dr_subordinate'.
    dry_run - If set, do not modify zk
    """
    try:
        if replica_type not in [host_utils.REPLICA_ROLE_DR_SLAVE,
                                host_utils.REPLICA_ROLE_SLAVE]:
            raise Exception('Invalid value "{}" for argument '
                            "replica_type").format(replica_type)

        log.info('Instance is {}'.format(instance))
        mysql_lib.assert_replication_sanity(instance)
        mysql_lib.assert_replication_unlagged(
            instance,
            mysql_lib.REPLICATION_TOLERANCE_NORMAL)
        main = mysql_lib.get_main_from_instance(instance)

        zk_local = host_utils.MysqlZookeeper()
        kazoo_client = environment_specific.get_kazoo_client()
        if not kazoo_client:
            raise Exception('Could not get a zk connection')

        if main not in zk_local.get_all_mysql_instances_by_type(
                    host_utils.REPLICA_ROLE_MASTER):
            raise Exception('Instance {} is not a main in zk'
                            ''.format(main))

        log.info('Detected main of {instance} '
                 'as {main}'.format(instance=instance,
                                      main=main))

        replica_set = zk_local.get_replica_set_from_instance(main)
        log.info('Detected replica_set as {}'.format(replica_set))
        old_instance = zk_local.get_mysql_instance_from_replica_set(
                           replica_set,
                           repl_type=replica_type)

        if replica_type == host_utils.REPLICA_ROLE_SLAVE:
            (zk_node,
             parsed_data, version) = get_zk_node_for_replica_set(kazoo_client,
                                                                 replica_set)
            log.info('Replica set {replica_set} is held in zk_node '
                     '{zk_node}'.format(zk_node=zk_node,
                                        replica_set=replica_set))
            log.info('Existing config:')
            log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
            new_data = copy.deepcopy(parsed_data)
            new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]['host'] = \
                instance.hostname
            new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]['port'] = \
                instance.port
            log.info('New config:')
            log.info(pprint.pformat(remove_auth(new_data[replica_set])))

            if new_data == parsed_data:
                raise Exception('No change would be made to zk, '
                                'will not write new config')
            elif dry_run:
                log.info('dry_run is set, therefore not modifying zk')
            else:
                log.info('Pushing new configuration for '
                         '{replica_set}:'.format(replica_set=replica_set))
                kazoo_client.set(zk_node, simplejson.dumps(new_data), version)
        elif replica_type == host_utils.REPLICA_ROLE_DR_SLAVE:
            znode_data, dr_meta = kazoo_client.get(environment_specific.DR_ZK)
            parsed_data = simplejson.loads(znode_data)
            new_data = copy.deepcopy(parsed_data)
            if replica_set in parsed_data:
                log.info('Existing dr config:')
                log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
            else:
                log.info('Replica set did not previously have a dr subordinate')

            new_data[replica_set] = \
                {host_utils.REPLICA_ROLE_DR_SLAVE: {'host': instance.hostname,
                                                    'port': instance.port}}
            log.info('New dr config:')
            log.info(pprint.pformat(remove_auth(new_data[replica_set])))

            if new_data == parsed_data:
                raise Exception('No change would be made to zk, '
                                'will not write new config')
            elif dry_run:
                log.info('dry_run is set, therefore not modifying zk')
            else:
                log.info('Pushing new dr configuration for '
                         '{replica_set}:'.format(replica_set=replica_set))
                kazoo_client.set(environment_specific.DR_ZK,
                                 simplejson.dumps(new_data), dr_meta.version)
        else:
            # we should raise an exception above rather than getting to here
            pass
        if not dry_run:
            log.info('Stopping replication and event scheduler on {} '
                     'being taken out of use'.format(old_instance))
            try:
                mysql_lib.stop_replication(old_instance)
                mysql_lib.stop_event_scheduler(old_instance)
            except:
                log.info('Could not stop replication on {}'
                         ''.format(old_instance))

    except Exception, e:
        log.exception(e)
        raise


def swap_main_and_subordinate(instance, dry_run):
    """ Swap a main and subordinate in zk. Warning: this does not sanity checks
        and does nothing more than update zk. YOU HAVE BEEN WARNED!

    Args:
    instance - An instance in the replica set. This function will figure
               everything else out.
    dry_run - If set, do not modify configuration.
    """
    zk_local = host_utils.MysqlZookeeper()
    kazoo_client = environment_specific.get_kazoo_client()
    if not kazoo_client:
        raise Exception('Could not get a zk connection')

    log.info('Instance is {}'.format(instance))

    replica_set = zk_local.get_replica_set_from_instance(instance)
    log.info('Detected replica_set as {}'.format(replica_set))

    (zk_node,
     parsed_data,
     version) = get_zk_node_for_replica_set(kazoo_client, replica_set)
    log.info('Replica set {replica_set} is held in zk_node '
             '{zk_node}'.format(zk_node=zk_node,
                                replica_set=replica_set))

    log.info('Existing config:')
    log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
    new_data = copy.deepcopy(parsed_data)
    new_data[replica_set][host_utils.REPLICA_ROLE_MASTER] = \
        parsed_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]
    new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE] = \
        parsed_data[replica_set][host_utils.REPLICA_ROLE_MASTER]

    log.info('New config:')
    log.info(pprint.pformat(remove_auth(new_data[replica_set])))

    if new_data == parsed_data:
        raise Exception('No change would be made to zk, '
                        'will not write new config')
    elif dry_run:
        log.info('dry_run is set, therefore not modifying zk')
    else:
        log.info('Pushing new configuration for '
                 '{replica_set}:'.format(replica_set=replica_set))
        kazoo_client.set(zk_node, simplejson.dumps(new_data), version)


def swap_subordinate_and_dr_subordinate(instance, dry_run):
    """ Swap a subordinate and a dr_subordinate in zk

    Args:
    instance - An instance that is either a subordinate or dr_subordinate
    """
    zk_local = host_utils.MysqlZookeeper()
    kazoo_client = environment_specific.get_kazoo_client()
    if not kazoo_client:
        raise Exception('Could not get a zk connection')

    log.info('Instance is {}'.format(instance))
    replica_set = zk_local.get_replica_set_from_instance(instance)

    log.info('Detected replica_set as {}'.format(replica_set))
    (zk_node,
     parsed_data,
     version) = get_zk_node_for_replica_set(kazoo_client, replica_set)
    log.info('Replica set {replica_set} is held in zk_node '
             '{zk_node}'.format(zk_node=zk_node,
                                replica_set=replica_set))

    log.info('Existing config:')
    log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
    new_data = copy.deepcopy(parsed_data)

    dr_znode_data, dr_meta = kazoo_client.get(environment_specific.DR_ZK)
    dr_parsed_data = simplejson.loads(dr_znode_data)
    new_dr_data = copy.deepcopy(dr_parsed_data)
    if replica_set not in parsed_data:
        raise Exception('Replica set {replica_set} is not present '
                        'in dr_node'.format(replica_set=replica_set))
    log.info('Existing dr config:')
    log.info(pprint.pformat(remove_auth(dr_parsed_data[replica_set])))

    new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE] = \
        dr_parsed_data[replica_set][host_utils.REPLICA_ROLE_DR_SLAVE]
    new_dr_data[replica_set][host_utils.REPLICA_ROLE_DR_SLAVE] = \
        parsed_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]

    log.info('New config:')
    log.info(pprint.pformat(remove_auth(new_data[replica_set])))

    log.info('New dr config:')
    log.info(pprint.pformat(remove_auth(new_dr_data[replica_set])))

    if dry_run:
        log.info('dry_run is set, therefore not modifying zk')
    else:
        log.info('Pushing new configuration for '
                 '{replica_set}:'.format(replica_set=replica_set))
        kazoo_client.set(zk_node, simplejson.dumps(new_data), version)
        try:
            kazoo_client.set(environment_specific.DR_ZK,
                             simplejson.dumps(new_dr_data), dr_meta.version)
        except:
            raise Exception('DR node is incorrect due to a different change '
                            'blocking this change.  Manual intervention '
                            'is required.')


def update_host_replacement_log(conn, instance_id):
    """ Mark a replacement as completed

    conn - A connection to the reporting server
    instance - The replacement instance
    """
    cursor = conn.cursor()
    sql = ("UPDATE mysqlops.host_replacement_log "
           "SET is_completed = 1 "
           "WHERE new_instance = %(new_instance)s ")
    params = {'new_instance': instance_id}
    cursor.execute(sql, params)
    log.info(cursor._executed)
    conn.commit()


if __name__ == "__main__":
    main()

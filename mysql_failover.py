#!/usr/bin/env python
import argparse
import logging
import os
import time
import uuid
import MySQLdb

import launch_replacement_db_host
import modify_mysql_zk
import fence_server
from lib import mysql_lib
from lib import host_utils
from lib import environment_specific

MAX_ZK_WRITE_ATTEMPTS = 5
MAX_FENCE_ATTEMPTS = 2
WAIT_TIME_CONFIRM_QUIESCE = 10

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('instance',
                        help='The main to be demoted')
    parser.add_argument('--trust_me_its_dead',
                        help=('You say you know what you are doing. We are '
                              'going to trust you and hope for the best'),
                        default=False,
                        action='store_true')
    parser.add_argument('--ignore_dr_subordinate',
                        help=('Need to promote, but already have a dead '
                              'dr_subordinate? This option is what you looking '
                              'for. The dr_subordinate will be completely '
                              'ignored.'),
                        default=False,
                        action='store_true')
    parser.add_argument('--dry_run',
                        help=('Do not actually run a promotion, just run '
                              'safety checks, etc...'),
                        default=False,
                        action='store_true')
    parser.add_argument('--skip_lock',
                        help=('Do not take a promotion lock. Scary.'),
                        default=False,
                        action='store_true')
    parser.add_argument('--gtid_migrate',
                        help=('Set this flag if migrating to a GTID-based '
                              'setup from non-GTID'),
                        default=False,
                        action='store_true')
    parser.add_argument('--kill_old_main',
                        help=('If we can not get the main into read_only, '
                              'send a mysqladmin kill to the old main.'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()

    instance = host_utils.HostAddr(args.instance)
    mysql_failover(instance, args.dry_run, args.skip_lock,
                   args.ignore_dr_subordinate, args.trust_me_its_dead,
                   args.kill_old_main, args.gtid_migrate)


def mysql_failover(main, dry_run, skip_lock, ignore_dr_subordinate,
                   trust_me_its_dead, kill_old_main, gtid_migrate):
    """ Promote a new MySQL main

    Args:
    main - Hostaddr object of the main instance to be demoted
    dry_run - Do not change state, just do sanity testing and exit
    skip_lock - Do not take a promotion lock
    ignore_dr_subordinate - Ignore the existance of a dr_subordinate
    trust_me_its_dead - Do not test to see if the main is dead
    kill_old_main - Send a mysqladmin kill command to the old main
    gtid_migrate - Set this if we're failing over to GTID for the
                   first time.
    Returns:
    new_main - The new main server
    """
    log.info('Main to demote is {}'.format(main))

    zk = host_utils.MysqlZookeeper()
    if zk.get_replica_type_from_instance(main) != host_utils.REPLICA_ROLE_MASTER:
        raise Exception('Instance {} is not a main'.format(main))

    replica_set = zk.get_replica_set_from_instance(main)
    log.info('Replica set is detected as {}'.format(replica_set))

    # take a lock here to make sure nothing changes underneath us
    if not skip_lock and not dry_run:
        log.info('Taking promotion lock on replica set')
        lock_identifier = get_promotion_lock(replica_set)
    else:
        lock_identifier = None

    # giant try. If there any problems we roll back from the except
    try:
        main_conn = False
        subordinate = zk.get_mysql_instance_from_replica_set(replica_set=replica_set,
                                                       repl_type=host_utils.REPLICA_ROLE_SLAVE)
        log.info('Subordinate/new main is detected as {}'.format(subordinate))

        if ignore_dr_subordinate:
            log.info('Intentionally ignoring a dr_subordinate')
            dr_subordinate = None
        else:
            dr_subordinate = zk.get_mysql_instance_from_replica_set(replica_set,
                                                              host_utils.REPLICA_ROLE_DR_SLAVE)
        log.info('DR subordinate is detected as {}'.format(dr_subordinate))
        if dr_subordinate:
            if dr_subordinate == subordinate:
                raise Exception('Subordinate and dr_subordinate appear to be the same')

            replicas = set([subordinate, dr_subordinate])
        else:
            replicas = set([subordinate])

        # We use main_conn as a mysql connection to the main server, if
        # it is False, the main is dead
        if trust_me_its_dead:
            main_conn = None
        else:
            main_conn = is_main_alive(main, replicas)

        # Test to see if the subordinate is setup for replication. If not, we are hosed
        log.info('Testing to see if Subordinate/new main is setup to write '
                 'replication logs')
        mysql_lib.get_main_status(subordinate)

        if kill_old_main and not dry_run:
            log.info('Killing old main, we hope you know what you are doing')
            mysql_lib.shutdown_mysql(main)
            main_conn = None

        if main_conn:
            log.info('Main is considered alive')
            dead_main = False
            confirm_max_replica_lag(replicas,
                                    mysql_lib.REPLICATION_TOLERANCE_NORMAL,
                                    dead_main)
        else:
            log.info('Main is considered dead')
            dead_main = True
            confirm_max_replica_lag(replicas,
                                    mysql_lib.REPLICATION_TOLERANCE_LOOSE,
                                    dead_main)

        if dry_run:
            log.info('In dry_run mode, so exiting now')
            # Using os._exit in order to not get catch in the giant try
            os._exit(environment_specific.DRY_RUN_EXIT_CODE)

        log.info('Preliminary sanity checks complete, starting promotion')

        if main_conn:
            log.info("We kill a checksum if it is running")
            if not host_utils.kill_checksum(main):
                log.info("Give it sometime to be gone")
                time.sleep(2)
            log.info('Setting read_only on main')
            mysql_lib.set_global_variable(main, 'read_only', True)
            log.info('Confirming no writes to old main')
            # If there are writes with the main in read_only mode then the
            # promotion can not proceed.
            # A likely reason is a client has the SUPER privilege.
            confirm_no_writes(main)
            log.info('Waiting for replicas to be caught up')
            confirm_max_replica_lag(replicas,
                                    mysql_lib.REPLICATION_TOLERANCE_NONE,
                                    dead_main,
                                    True,
                                    mysql_lib.NORMAL_HEARTBEAT_LAG)

            # since the main is alive, we can check for these.
            # and we want to wait until the replica is in sync.
            errant_trx = mysql_lib.find_errant_trx(subordinate, main)
            if errant_trx:
                log.warning('Errant transactions found!  Repairing via main.')
                mysql_lib.fix_errant_trx(errant_trx, main, True)
            else:
                log.info('No errant transactions detected.')

            log.info('Setting up replication from old main ({main}) '
                     'to new main ({subordinate})'.format(main=main,
                                                      subordinate=subordinate))
            mysql_lib.setup_replication(new_main=subordinate, new_replica=main,
                                        auto_pos=(not gtid_migrate))
        else:
            # if the main is dead, we don't try to fix any errant trx on
            # the main.  however, we may need to fix them on the dr_subordinate,
            # if one exists.

            log.info('Starting up a zk connection to make sure we can connect')
            kazoo_client = environment_specific.get_kazoo_client()
            if not kazoo_client:
                raise Exception('Could not connect to zk')

            log.info('Confirming replica has processed all replication logs')
            confirm_no_writes(subordinate)
            log.info('Looks like no writes being processed by replica via '
                     'replication or other means')
            if len(replicas) > 1:
                log.info('Confirming replica servers are synced')
                confirm_max_replica_lag(replicas,
                                        mysql_lib.REPLICATION_TOLERANCE_LOOSE,
                                        dead_main,
                                        True)
    except:
        log.info('Starting rollback')
        if main_conn:
            log.info('Releasing read_only on old main')
            mysql_lib.set_global_variable(main, 'read_only', False)

            log.info('Clearing replication settings on old main')
            mysql_lib.reset_subordinate(main)
        if lock_identifier:
            log.info('Releasing promotion lock')
            release_promotion_lock(lock_identifier)
        log.info('Rollback complete, reraising exception')
        raise

    if dr_subordinate:
        try:
            mysql_lib.setup_replication(new_main=subordinate, new_replica=dr_subordinate,
                                        auto_pos=(not gtid_migrate))
            # anything on the subordinate that the dr_subordinate still doesn't have?
            errant_trx = mysql_lib.find_errant_trx(subordinate, dr_subordinate)
            if errant_trx:
                log.warning("Repairing subordinate's errant transactions on dr_subordinate.")
                mysql_lib.fix_errant_trx(errant_trx, dr_subordinate, False)
            else:
                log.info("No subordinate -> dr_subordinate errant trx detected.")

            # what about anything on the dr_subordinate that the subordinate doesn't have?
            errant_trx = mysql_lib.find_errant_trx(dr_subordinate, subordinate)
            if errant_trx:
                log.warning("Reparing dr_subordinate errant transactions on subordinate.")
                mysql_lib.fix_errant_trx(errant_trx, subordinate, False)
            else:
                log.info("No dr_subordinate -> subordinate errant trx detected.")

        except Exception as e:
            log.error(e)
            log.error('Setting up replication on the dr_subordinate failed. '
                      'Failing forward!')


    log.info('Updating zk')
    zk_write_attempt = 0
    while True:
        try:
            modify_mysql_zk.swap_main_and_subordinate(subordinate, dry_run=False)
            break
        except:
            if zk_write_attempt > MAX_ZK_WRITE_ATTEMPTS:
                log.info('Final failure writing to zk, bailing')
                raise
            else:
                log.info('Write to zk failed, trying again')
                zk_write_attempt = zk_write_attempt + 1

    log.info('Removing read_only from new main')
    mysql_lib.set_global_variable(subordinate, 'read_only', False)
    log.info('Removing replication configuration from new main')
    mysql_lib.reset_subordinate(subordinate)
    # fence dead server
    if dead_main:
        # for some weird case when local config file is not
        # updated with new zk config but the old main is dead
        # already we simply FORCE-fence it here
        fence_server.add_fence_to_host(main, dry_run, force=True)

    if lock_identifier:
        log.info('Releasing promotion lock')
        release_promotion_lock(lock_identifier)

    log.info('Failover complete')

    # we don't really care if this fails, but we'll print a message anyway.
    try:
        environment_specific.generic_json_post(
            environment_specific.CHANGE_FEED_URL,
            {'type': 'MySQL Failover',
             'environment': replica_set,
             'description': "Failover from {m} to {s}".format(m=main, s=subordinate),
             'author': host_utils.get_user(),
             'automation': False,
             'source': "mysql_failover.py on {}".format(host_utils.HOSTNAME)})
    except Exception as e:
        log.warning("Failover completed, but change feed "
                    "not updated: {}".format(e))

    if not main_conn:
        log.info('As main is dead, will try to launch a replacement. Will '
                 'sleep 20 seconds first to let things settle')
        time.sleep(20)
        launch_replacement_db_host.launch_replacement_db_host(main)


def get_promotion_lock(replica_set):
    """ Take a promotion lock

    Args:
    replica_set - The replica set to take the lock against

    Returns:
    A unique identifer for the lock
    """
    lock_identifier = str(uuid.uuid4())
    log.info('Promotion lock identifier is '
             '{lock_identifier}'.format(lock_identifier=lock_identifier))

    conn = mysql_lib.get_mysqlops_connections()

    log.info('Releasing any expired locks')
    release_expired_promotion_locks(conn)

    log.info('Checking existing locks')
    check_promotion_lock(conn, replica_set)

    log.info('Taking lock against replica set: '
             '{replica_set}'.format(replica_set=replica_set))
    params = {'lock': lock_identifier,
              'localhost': host_utils.HOSTNAME,
              'replica_set': replica_set,
              'user': host_utils.get_user()}
    sql = ("INSERT INTO mysqlops.promotion_locks "
           "SET "
           "lock_identifier = %(lock)s, "
           "lock_active = 'active', "
           "created_at = NOW(), "
           "expires = NOW() + INTERVAL 12 HOUR, "
           "released = NULL, "
           "replica_set = %(replica_set)s, "
           "promoting_host = %(localhost)s, "
           "promoting_user = %(user)s ")
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
    log.info(cursor._executed)
    return lock_identifier


def release_expired_promotion_locks(lock_conn):
    """ Release any locks which have expired

    Args:
    lock_conn - a mysql connection to the mysql instance storing locks
    """
    cursor = lock_conn.cursor()
    # There is a unique index on (replica_set,lock_active), so a replica set
    # may not have more than a single active promotion in flight. We therefore
    # can not set lock_active = 'inactive' as only a single entry would be
    # allowed for inactive.
    sql = ('UPDATE mysqlops.promotion_locks '
           'SET lock_active = NULL '
           'WHERE expires < now()')
    cursor.execute(sql)
    lock_conn.commit()
    log.info(cursor._executed)


def check_promotion_lock(lock_conn, replica_set):
    """ Confirm there are no active locks that would block taking a
        promotion lock

    Args:
    lock_conn - a mysql connection to the mysql instance storing locks
    replica_set - the replica set that should be locked
    """
    cursor = lock_conn.cursor()
    params = {'replica_set': replica_set}
    sql = ('SELECT lock_identifier, promoting_host, promoting_user '
           'FROM mysqlops.promotion_locks '
           "WHERE lock_active = 'active' AND "
           "replica_set = %(replica_set)s")
    cursor.execute(sql, params)
    ret = cursor.fetchone()
    if ret is not None:
        log.error('Lock is already held by {lock}'.format(lock=ret))
        log.error(('To relase this lock you can connect to the mysqlops '
                   'db by running: '))
        log.error('/usr/local/bin/mysql_utils/mysql_cli.py mysqlopsdb001 '
                  '-p read-write ')
        log.error('And then running the following query:')
        log.error(('UPDATE mysqlops.promotion_locks '
                   'SET lock_active = NULL AND released = NOW() '
                   'WHERE lock_identifier = '
                  "'{lock}';".format(lock=ret['lock_identifier'])))
        raise Exception('Can not take promotion lock')


def release_promotion_lock(lock_identifier):
    """ Release a promotion lock

    Args:
    lock_identifier - The lock to release
    """
    conn = mysql_lib.get_mysqlops_connections()
    cursor = conn.cursor()

    params = {'lock_identifier': lock_identifier}
    sql = ('UPDATE mysqlops.promotion_locks '
           'SET lock_active = NULL AND released = NOW() '
           'WHERE lock_identifier = %(lock_identifier)s')
    cursor.execute(sql, params)
    conn.commit()
    log.info(cursor._executed)


def confirm_max_replica_lag(replicas, lag_tolerance, dead_main,
                            replicas_synced=False, timeout=0):
    """ Test replication lag

    Args:
    replicas - A set of hostaddr object to be tested for replication lag
    max_lag - Max computed replication lag in seconds. If 0 is supplied,
              then exec position is compared from replica servers to the
              main rather than using a computed second behind as the
              heartbeat will be blocked by read_only.
    replicas_synced - Replica servers must have executed to the same
                      position in the binary log.
    timeout - How long to wait for replication to be in the desired state
    """
    start = time.time()
    if dead_main:
        replication_checks = set([mysql_lib.CHECK_SQL_THREAD,
                                  mysql_lib.CHECK_CORRECT_MASTER])
    else:
        replication_checks = mysql_lib.ALL_REPLICATION_CHECKS

    while True:
        acceptable = True
        for replica in replicas:
            # Confirm threads are running, expected main
            try:
                mysql_lib.assert_replication_sanity(replica, replication_checks)
            except Exception as e:
                log.warning(e)
                log.info('Trying to restart replication, then '
                         'sleep 20 seconds')
                mysql_lib.restart_replication(replica)
                time.sleep(20)
                mysql_lib.assert_replication_sanity(replica, replication_checks)

            try:
                mysql_lib.assert_replication_unlagged(replica, lag_tolerance, dead_main)
            except Exception as e:
                log.warning(e)
                acceptable = False

        if replicas_synced and not confirm_replicas_in_sync(replicas):
            acceptable = False
            log.warning('Replica servers are not in sync and replicas_synced '
                        'is set')

        if acceptable:
            return
        elif (time.time() - start) > timeout:
            raise Exception('Replication is not in an acceptable state on '
                            'replica {r}'.format(r=replica))
        else:
            log.info('Sleeping for 5 second to allow replication to catch up')
            time.sleep(5)


def is_main_alive(main, replicas):
    """ Determine if the main is alive

    The function will:
    1. Attempt to connect to the main via the mysql protcol. If successful
       the main is considered alive.
    2. If #1 fails, check the io thread of the replica instance(s). If the io
       thread is not running, the main will be considered dead. If step #1
       fails and step #2 succeeds, we are in a weird state and will throw an
       exception.

    Args:
    main - A hostaddr object for the main instance
    replicas -  A set of hostaddr objects for the replica instances

    Returns:
    A mysql connection to the main if the main is alive, False otherwise.
    """
    if len(replicas) == 0:
        raise Exception('At least one replica must be present to determine '
                        'a main is dead')
    try:
        main_conn = mysql_lib.connect_mysql(main)
        return main_conn
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != mysql_lib.MYSQL_ERROR_CONN_HOST_ERROR:
            raise
        main_conn = False
        log.info('Unable to connect to current main {main} from '
                 '{hostname}, will check replica servers beforce declaring '
                 'the main dead'.format(main=main,
                                          hostname=host_utils.HOSTNAME))
    except:
        log.info('This is an unknown connection error. If you are very sure '
                 'that the main is dead, please put a "return False" at the '
                 'top of is_main_alive and then send rwultsch a stack trace')
        raise

    # We can not get a connection to the main, so poll the replica servers
    for replica in replicas:
        # If replication has not hit a timeout, a dead main can still have
        # a replica which thinks it is ok. "STOP SLAVE; START SLAVE" followed
        # by a sleep will get us truthyness.
        mysql_lib.restart_replication(replica)
        try:
            mysql_lib.assert_replication_sanity(replica)
            raise Exception('Replica {replica} thinks it can connect to '
                            'main {main}, but failover script can not. '
                            'Possible network partition!'
                            ''.format(replica=replica,
                                      main=main))
        except:
            # The exception is expected in this case
            pass
        log.info('Replica {replica} also can not connect to main '
                 '{main}.'.format(replica=replica,
                                    main=main))
    return False


def confirm_no_writes(instance):
    """ Confirm that a server is not receiving any writes

    Args:
    conn - A mysql connection
    """
    mysql_lib.enable_and_flush_activity_statistics(instance)
    log.info('Waiting {length} seconds to confirm instance is no longer '
             'accepting writes'.format(length=WAIT_TIME_CONFIRM_QUIESCE))
    time.sleep(WAIT_TIME_CONFIRM_QUIESCE)
    db_activity = mysql_lib.get_dbs_activity(instance)

    active_db = set()
    for db in db_activity:
        if db_activity[db]['ROWS_CHANGED'] != 0:
            active_db.add(db)

    if active_db:
        raise Exception('DB {dbs} has been modified when it should have '
                        'no activity'.format(dbs=active_db))

    log.info('No writes after sleep, looks like we are good to go')


def confirm_replicas_in_sync(replicas):
    """ Confirm that all replicas are in sync in terms of replication

    Args:
    replicas - A set of hostAddr objects
    """
    replication_progress = set()
    for replica in replicas:
        subordinate_status = mysql_lib.get_subordinate_status(replica)
        replication_progress.add(':'.join((subordinate_status['Relay_Main_Log_File'],
                                           str(subordinate_status['Exec_Main_Log_Pos']))))

    if len(replication_progress) == 1:
        return True
    else:
        return False


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()

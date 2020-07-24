import json
import datetime
import MySQLdb
import MySQLdb.cursors
import re
import time
import warnings

import _mysql_exceptions

import host_utils
import mysql_connect
from lib import environment_specific


# Max IO thread lag in bytes. If more than NORMAL_IO_LAG refuse to modify zk, etc
# 10k bytes of lag is just a few seconds normally
NORMAL_IO_LAG = 10485760
# Max lag in seconds. If more than NORMAL_HEARTBEAT_LAG refuse to modify zk, or
# attempt a live main failover
NORMAL_HEARTBEAT_LAG = 120
HEARTBEAT_SAFETY_MARGIN = 10
# Max lag in second for a dead main failover
LOOSE_HEARTBEAT_LAG = 3600

CHECK_SQL_THREAD = 'sql'
CHECK_IO_THREAD = 'io'
CHECK_CORRECT_MASTER = 'main'
ALL_REPLICATION_CHECKS = set([CHECK_SQL_THREAD,
                              CHECK_IO_THREAD,
                              CHECK_CORRECT_MASTER])
REPLICATION_THREAD_SQL = 'SQL'
REPLICATION_THREAD_IO = 'IO'
REPLICATION_THREAD_ALL = 'ALL'
REPLICATION_THREAD_TYPES = set([REPLICATION_THREAD_SQL,
                                REPLICATION_THREAD_IO,
                                REPLICATION_THREAD_ALL])

AUTH_FILE = '/var/config/config.services.mysql_auth'
CONNECT_TIMEOUT = 2
INVALID = 'INVALID'
METADATA_DB = 'test'
MYSQL_DATETIME_TO_PYTHON = '%Y-%m-%dT%H:%M:%S.%f'
MYSQLADMIN = '/usr/bin/mysqladmin'
MYSQL_ERROR_CANT_CREATE_WRITE_TO_FILE = 1
MYSQL_ERROR_ACCESS_DENIED = 1045
MYSQL_ERROR_CONN_HOST_ERROR = 2003
MYSQL_ERROR_HOST_ACCESS_DENIED = 1130
MYSQL_ERROR_NO_SUCH_TABLE = 1146
MYSQL_ERROR_NO_DEFINED_GRANT = 1141
MYSQL_ERROR_NO_SUCH_THREAD = 1094
MYSQL_ERROR_UNKNOWN_VAR = 1193
MYSQL_ERROR_FUNCTION_EXISTS = 1125
MYSQL_VERSION_COMMAND = '/usr/sbin/mysqld --version'
REPLICATION_TOLERANCE_NONE = 'None'
REPLICATION_TOLERANCE_NORMAL = 'Normal'
REPLICATION_TOLERANCE_LOOSE = 'Loose'


class ReplicationError(Exception):
    pass


class AuthError(Exception):
    pass


class InvalidVariableForOperation(Exception):
    pass


log = environment_specific.setup_logging_defaults(__name__)


def get_all_mysql_grants():
    """Fetch all MySQL grants

    Returns:
    A dictionary describing all MySQL grants.

    Example:
    {'`admin2`@`%`': {'grant_option': True,
                  'password': u'REDACTED',
                  'privileges': u'ALL PRIVILEGES',
                  'source_host': '%',
                  'username': u'admin2'},
     '`admin`@`%`': {'grant_option': True,
                 'password': u'REDACTED',
                 'privileges': u'ALL PRIVILEGES',
                 'source_host': '%',
                 'username': u'admin'},
     '`etl2`@`%`': {'grant_option': False,
                'password': u'REDACTED',
                'privileges': u'SELECT, SHOW DATABASES',
                'source_host': '%',
                'username': u'etl2'},
    ...
    """
    grants = {}
    for _, role in get_mysql_auth_roles().iteritems():
        source_hosts = role.get('source_hosts', '%')
        grant_option = role.get('grant_option', False)
        privileges = role['privileges']

        for user in role['users']:
            key = '`{user}`@`{host}`'.format(user=user['username'],
                                             host=source_hosts)

            if key in grants.keys():
                raise AuthError('Duplicate username defined for %s' % key)
            grants[key] = {'username': user['username'].encode('ascii', 'ignore'),
                           'password': user['password'].encode('ascii', 'ignore'),
                           'privileges': privileges.encode('ascii', 'ignore'),
                           'grant_option': grant_option,
                           'source_host': source_hosts.encode('ascii', 'ignore')}
    return grants


def get_mysql_user_for_role(role):
    """Fetch the credential for a role from a mysql role

    Args:
    role - a string of the name of the mysql role to use for username/password

    Returns:
    username - string of the username enabled for the role
    password - string of the password enabled for the role
    """
    grants = get_mysql_auth_roles()[role]
    for user in grants['users']:
        if user['enabled']:
            return user['username'], user['password']


def get_mysql_auth_roles():
    """Get all mysql roles from zk updater

    Returns:
    a dict describing the replication status.

    Example:
    {u'dataLayer': {u'privileges': u'SELECT',
                    u'users': [
                        {u'username': u'pbdataLayer',
                         u'password': u'REDACTED',
                         u'enabled': True},
                        {u'username': u'pbdataLayer2',
                         u'password': u'REDACTED',
                         u'enabled': False}]},
...
"""
    with open(AUTH_FILE) as f:
        json_grants = json.loads(f.read())
    return json_grants


def connect_mysql(instance, role='admin'):
    """Connect to a MySQL instance as admin

    Args:
    hostaddr - object describing which mysql instance to connect to
    role - a string of the name of the mysql role to use. A bootstrap role can
           be called for MySQL instances lacking any grants. This user does not
           exit in zk.

    Returns:
    a connection to the server as administrator
    """
    if role == 'bootstrap':
        socket = host_utils.get_cnf_setting('socket', instance.port)
        username = 'root'
        password = ''
        db = MySQLdb.connect(unix_socket=socket,
                             user=username,
                             passwd=password,
                             cursorclass=MySQLdb.cursors.DictCursor)

    else:
        username, password = get_mysql_user_for_role(role)
        db = MySQLdb.connect(host=instance.hostname,
                             port=instance.port,
                             user=username,
                             passwd=password,
                             cursorclass=MySQLdb.cursors.DictCursor,
                             connect_timeout=CONNECT_TIMEOUT)
    return db


def get_main_from_instance(instance):
    """ Determine if an instance thinks it is a subordinate and if so from where

    Args:
    instance - A hostaddr object

    Returns:
    main - A hostaddr object or None
    """
    try:
        ss = get_subordinate_status(instance)
    except ReplicationError:
        return None

    return host_utils.HostAddr(''.join((ss['Main_Host'],
                                        ':',
                                        str(ss['Main_Port']))))


def get_subordinate_status(instance):
    """ Get MySQL replication status

    Args:
    instance - A hostaddr object

    Returns:
    a dict describing the replication status.

    Example:
    {'Connect_Retry': 60L,
     'Exec_Main_Log_Pos': 98926487L,
     'Last_Errno': 0L,
     'Last_Error': '',
     'Last_IO_Errno': 0L,
     'Last_IO_Error': '',
     'Last_SQL_Errno': 0L,
     'Last_SQL_Error': '',
     'Main_Host': 'sharddb015e',
     'Main_Log_File': 'mysql-bin.000290',
     'Main_Port': 3306L,
     'Main_SSL_Allowed': 'No',
     'Main_SSL_CA_File': '',
     'Main_SSL_CA_Path': '',
     'Main_SSL_Cert': '',
     'Main_SSL_Cipher': '',
     'Main_SSL_Key': '',
     'Main_SSL_Verify_Server_Cert': 'No',
     'Main_Server_Id': 946544731L,
     'Main_User': 'replicant',
     'Read_Main_Log_Pos': 98926487L,
     'Relay_Log_File': 'mysqld_3306-relay-bin.000237',
     'Relay_Log_Pos': 98926633L,
     'Relay_Log_Space': 98926838L,
     'Relay_Main_Log_File': 'mysql-bin.000290',
     'Replicate_Do_DB': '',
     'Replicate_Do_Table': '',
     'Replicate_Ignore_DB': '',
     'Replicate_Ignore_Server_Ids': '',
     'Replicate_Ignore_Table': '',
     'Replicate_Wild_Do_Table': '',
     'Replicate_Wild_Ignore_Table': '',
     'Seconds_Behind_Main': 0L,
     'Skip_Counter': 0L,
     'Subordinate_IO_Running': 'Yes',
     'Subordinate_IO_State': 'Waiting for main to send event',
     'Subordinate_SQL_Running': 'Yes',
     'Until_Condition': 'None',
     'Until_Log_File': '',
     'Until_Log_Pos': 0L}
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    cursor.execute("SHOW SLAVE STATUS")
    subordinate_status = cursor.fetchone()
    if subordinate_status is None:
        raise ReplicationError('Instance {} is not a subordinate'.format(instance))
    return subordinate_status


def flush_main_log(instance):
    """ Flush binary logs

    Args:
    instance - a hostAddr obect
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    cursor.execute("FLUSH BINARY LOGS")


def get_main_status(instance):
    """ Get poisition of most recent write to main replication logs

    Args:
    instance - a hostAddr object

    Returns:
    a dict describing the main status

    Example:
    {'Binlog_Do_DB': '',
     'Binlog_Ignore_DB': '',
     'File': 'mysql-bin.019324',
     'Position': 61559L,
     'Executed_Gtid_Set': 'b27a8edf-eca1-11e6-99e4-0e695f0e3b16:1-151028417'
    }
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    cursor.execute("SHOW MASTER STATUS")
    main_status = cursor.fetchone()
    if main_status is None:
        raise ReplicationError('Server is not setup to write replication logs')
    return main_status


def get_gtid_subtract(instance, src_gtid_set, ref_gtid_set):
    """ Get poisition of most recent write to main replication logs

    Args:
    instance - a hostAddr object
    s_gtid_set: set of source gtid
    ref_gtid_set: set of reference gtid

    Returns:
    'bba7d5dd-fdfe-11e6-8b1e-12005f9b2b06:877821-877864'

    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    cursor.execute("SELECT GTID_SUBTRACT('{source}','{ref}') as "
                   "errant_trx".format(source=src_gtid_set, ref=ref_gtid_set))
    gtid_subtract = cursor.fetchone()
    return gtid_subtract['errant_trx']


def get_main_logs(instance):
    """ Get MySQL binary log names and size

    Args
    db - a connection to the server as administrator

    Returns
    A tuple of dicts describing the replication status.

    Example:
    ({'File_size': 104857859L, 'Log_name': 'mysql-bin.000281'},
     {'File_size': 104858479L, 'Log_name': 'mysql-bin.000282'},
     {'File_size': 104859420L, 'Log_name': 'mysql-bin.000283'},
     {'File_size': 104859435L, 'Log_name': 'mysql-bin.000284'},
     {'File_size': 104858059L, 'Log_name': 'mysql-bin.000285'},
     {'File_size': 104859233L, 'Log_name': 'mysql-bin.000286'},
     {'File_size': 104858895L, 'Log_name': 'mysql-bin.000287'},
     {'File_size': 104858039L, 'Log_name': 'mysql-bin.000288'},
     {'File_size': 104858825L, 'Log_name': 'mysql-bin.000289'},
     {'File_size': 104857726L, 'Log_name': 'mysql-bin.000290'},
     {'File_size': 47024156L, 'Log_name': 'mysql-bin.000291'})
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    cursor.execute("SHOW MASTER LOGS")
    main_status = cursor.fetchall()
    return main_status


def get_binlog_archiving_lag(instance):
    """ Get date of creation of most recent binlog archived

    Args:
    instance - a hostAddr object

    Returns:
    A datetime object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    sql = ("SELECT binlog_creation "
           "FROM {db}.{tbl} "
           "WHERE hostname= %(hostname)s  AND "
           "      port = %(port)s "
           "ORDER BY binlog_creation DESC "
           "LIMIT 1;").format(db=METADATA_DB,
                              tbl=environment_specific.BINLOG_ARCHIVING_TABLE_NAME)
    params = {'hostname': instance.hostname,
              'port': instance.port}
    cursor.execute(sql, params)
    res = cursor.fetchone()
    if res:
        return res['binlog_creation']
    else:
        return None


def calc_binlog_behind(log_file_num, log_file_pos, main_logs):
    """ Calculate replication lag in bytes

    Args:
    log_file_num - The integer of the binlog
    log_file_pos - The position inside of log_file_num
    main_logs - A tuple of dicts describing the replication status

    Returns:
    bytes_behind - bytes of lag across all log file
    binlogs_behind - number of binlogs lagged
    """
    binlogs_behind = 0
    bytes_behind = 0
    for binlog in main_logs:
        _, binlog_num = re.split('\.', binlog['Log_name'])
        if int(binlog_num) >= int(log_file_num):
            if binlog_num == log_file_num:
                bytes_behind += binlog['File_size'] - log_file_pos
            else:
                binlogs_behind += 1
                bytes_behind += binlog['File_size']
    return bytes_behind, binlogs_behind


def get_global_variables(instance):
    """ Get MySQL global variables

    Args:
    instance - A hostAddr object

    Returns:
    A dict with the key the variable name
    """
    conn = connect_mysql(instance)
    ret = dict()
    cursor = conn.cursor()
    cursor.execute("SHOW GLOBAL VARIABLES")
    list_variables = cursor.fetchall()
    for entry in list_variables:
        ret[entry['Variable_name']] = entry['Value']

    return ret


def get_dbs(instance):
    """ Get MySQL databases other than mysql, information_schema,
    performance_schema and test

    Args:
    instance - A hostAddr object

    Returns
    A set of databases
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = set()

    cursor.execute(' '.join(("SELECT schema_name",
                             "FROM information_schema.schemata",
                             "WHERE schema_name NOT IN('mysql',",
                             "                         'information_schema',",
                             "                         'performance_schema',",
                             "                         'test')",
                             "ORDER BY schema_name")))
    dbs = cursor.fetchall()
    for db in dbs:
        ret.add(db['schema_name'])
    return ret


def does_table_exist(instance, db, table):
    """ Return True if a given table exists in a given database.

    Args:
    instance - A hostAddr object
    db - A string that contains the database name we're looking for
    table - A string containing the name of the table we're looking for

    Returns:
    True if the table was found.
    False if not or there was an exception.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    table_exists = False

    try:
        sql = ("SELECT COUNT(*) AS cnt FROM information_schema.tables "
               "WHERE table_schema=%(db)s AND table_name=%(tbl)s")
        cursor.execute(sql, {'db': db, 'tbl': table})
        row = cursor.fetchone()
        if row['cnt'] == 1:
            table_exists = True
    except:
        # If it doesn't work, we can't know anything about the
        # state of the table.
        log.info('Ignoring an error checking for existance of '
                 '{db}.{table}'.format(db=db, table=table))

    return table_exists


def get_all_tables_by_instance(instance):
    """ Get a list of all of the tables on a given instance in all of
        the databases that we might be able to dump.  We intentionally
        omit anything that isn't InnoDB or MyISAM.

    Args:
        instance: a hostAddr object
    Returns:
        A set of fully-qualified table names.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    result = set()

    sql = ("SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS tbl FROM "
           "information_schema.tables WHERE TABLE_TYPE='BASE TABLE' "
           "AND ENGINE IN ('InnoDB', 'MyISAM')")

    cursor.execute(sql)
    for t in cursor.fetchall():
        result.add(t['tbl'])
    return result


def get_partitions_for_table(instance, db, table):
    """ Get a list of all partitions for a given table, if there are
        any.  Return them in a list.  We do not support subpartitions

        Args:
            instance - a hostAddr object
            db - A string containing the database name
            table - The table to investigate
        Returns:
            partition_list: A list of partition names, if there are any.
                            Otherwise, a one-element list containing 'None'.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    param = {'db': db,
             'tbl': table}
    partition_list = list()

    sql = ("SELECT DISTINCT PARTITION_NAME AS p FROM "
           "information_schema.partitions WHERE TABLE_SCHEMA=%(db)s "
           "AND TABLE_NAME=%(tbl)s AND PARTITION_NAME IS NOT NULL")
    cursor.execute(sql, param)
    for row in cursor.fetchall():
        partition_list.append(row['p'])

    if len(partition_list) == 0:
        partition_list.append(None)

    return partition_list


def get_tables(instance, db, skip_views=False):
    """ Get a list of tables and views in a given database or just
        tables.  Default to include views so as to maintain backward
        compatibility.

    Args:
    instance - A hostAddr object
    db - a string which contains a name of a db
    skip_views - true if we want tables only, false if we want everything

    Returns
    A set of tables
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = set()

    param = {'db': db}
    sql = ''.join(("SELECT TABLE_NAME ",
                   "FROM information_schema.tables ",
                   "WHERE TABLE_SCHEMA=%(db)s "))
    if skip_views:
        sql = sql + ' AND TABLE_TYPE="BASE TABLE" '

    cursor.execute(sql, param)
    for table in cursor.fetchall():
        ret.add(table['TABLE_NAME'])

    return ret


def get_columns_for_table(instance, db, table):
    """ Get a list of columns in a table

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db
    table - the name of the table to fetch columns

    Returns
    A list of columns
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = list()

    param = {'db': db,
             'table': table}
    sql = ("SELECT COLUMN_NAME "
           "FROM information_schema.columns "
           "WHERE TABLE_SCHEMA=%(db)s AND"
           "      TABLE_NAME=%(table)s")
    cursor.execute(sql, param)
    for column in cursor.fetchall():
        ret.append(column['COLUMN_NAME'])

    return ret


def setup_audit_plugin(instance):
    """ Install the audit log plugin.  Similarly to semisync, we may
        or may not use it, but we need to make sure that it's available.
        Audit plugin is available on 5.5 and above, which are all the
        versions we support.

        Args:
        instance - A hostaddr object
    """
    return 
    # this is intentional; we may re-enable the audit log at some 
    # later date, but for now, we just exit.

    conn = connect_mysql(instance)
    cursor = conn.cursor()

    try:
        cursor.execute("INSTALL PLUGIN audit_log SONAME 'audit_log.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        # plugin already loaded, nothing to do.
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
    conn.close()


def setup_semisync_plugins(instance):
    """ Install the semi-sync replication plugins.  We may or may
        not actually use them on any given replica set, but this
        ensures that we've got them.  Semi-sync exists on all versions
        of MySQL that we might support, but we'll never use it on 5.5.

        Args:
        instance - A hostaddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    version = get_global_variables(instance)['version']
    if version[0:3] == '5.5':
        return

    try:
        cursor.execute("INSTALL PLUGIN rpl_semi_sync_main SONAME 'semisync_main.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
        # already loaded, no work to do

    try:
        cursor.execute("INSTALL PLUGIN rpl_semi_sync_subordinate SONAME 'semisync_subordinate.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise


def setup_response_time_metrics(instance):
    """ Add Query Response Time Plugins

    Args:
    instance -  A hostaddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    version = get_global_variables(instance)['version']
    if version[0:3] < '5.6':
        return

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME_AUDIT SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
        # already loaded, no work to do

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME_READ SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME_WRITE SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
    cursor.execute("SET GLOBAL QUERY_RESPONSE_TIME_STATS=ON")


def enable_and_flush_activity_statistics(instance):
    """ Reset counters for table statistics

    Args:
    instance - a hostAddr obect
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    global_vars = get_global_variables(instance)
    if global_vars['userstat'] != 'ON':
        set_global_variable(instance, 'userstat', True)

    sql = 'FLUSH TABLE_STATISTICS'
    log.info(sql)
    cursor.execute(sql)

    sql = 'FLUSH USER_STATISTICS'
    log.info(sql)
    cursor.execute(sql)


def get_dbs_activity(instance):
    """ Return rows read and changed from a MySQL instance by db

    Args:
    instance - a hostAddr object

    Returns:
    A dict with a key of the db name and entries for rows read and rows changed
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = dict()

    global_vars = get_global_variables(instance)
    if global_vars['userstat'] != 'ON':
        raise InvalidVariableForOperation('Userstats must be enabled on ',
                                          'for table_statistics to function. '
                                          'Perhaps run "SET GLOBAL userstat = '
                                          'ON" to fix this.')

    sql = ("SELECT SCHEMA_NAME, "
           "    SUM(ROWS_READ) AS 'ROWS_READ', "
           "    SUM(ROWS_CHANGED) AS 'ROWS_CHANGED' "
           "FROM information_schema.SCHEMATA "
           "LEFT JOIN information_schema.TABLE_STATISTICS "
           "    ON SCHEMA_NAME=TABLE_SCHEMA "
           "GROUP BY SCHEMA_NAME ")
    cursor.execute(sql)
    raw_activity = cursor.fetchall()
    for row in raw_activity:
        if row['ROWS_READ'] is None:
            row['ROWS_READ'] = 0

        if row['ROWS_CHANGED'] is None:
            row['ROWS_CHANGED'] = 0

        ret[row['SCHEMA_NAME']] = {'ROWS_READ': int(row['ROWS_READ']),
                                   'ROWS_CHANGED': int(row['ROWS_CHANGED'])}
    return ret


def get_user_activity(instance):
    """ Return information about activity broken down by mysql user accoutn

    Args:
    instance - a hostAddr object

    Returns:
    a dict of user activity since last flush
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = dict()

    global_vars = get_global_variables(instance)
    if global_vars['userstat'] != 'ON':
        raise InvalidVariableForOperation('Userstats must be enabled on ',
                                          'for table_statistics to function. '
                                          'Perhaps run "SET GLOBAL userstat = '
                                          'ON" to fix this.')

    sql = 'SELECT * FROM information_schema.USER_STATISTICS'
    cursor.execute(sql)
    raw_activity = cursor.fetchall()
    for row in raw_activity:
        user = row['USER']
        del(row['USER'])
        ret[user] = row

    return ret


def get_connected_users(instance):
    """ Get all currently connected users

    Args:
    instance - a hostAddr object

    Returns:
    a set of users
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ("SELECT user "
           "FROM information_schema.processlist "
           "GROUP BY user")
    cursor.execute(sql)
    results = cursor.fetchall()

    ret = set()
    for result in results:
        ret.add(result['user'])

    return ret


def show_create_table(instance, db, table, standardize=True):
    """ Get a standardized CREATE TABLE statement

    Args:
    instance - a hostAddr object
    db - the MySQL database to run against
    table - the table on the db database to run against
    standardize - Remove AUTO_INCREMENT=$NUM and similar

    Returns:
    A string of the CREATE TABLE statement
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    try:
        cursor.execute('SHOW CREATE TABLE `{db}`.`{table}`'.format(table=table,
                                                                   db=db))
        ret = cursor.fetchone()['Create Table']
        if standardize is True:
            ret = re.sub('AUTO_INCREMENT=[0-9]+ ', '', ret)
    except MySQLdb.ProgrammingError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_NO_SUCH_TABLE:
            raise
        ret = ''

    return ret


def create_db(instance, db):
    """ Create a database if it does not already exist

    Args:
    instance - a hostAddr object
    db - the name of the db to be created
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ('CREATE DATABASE IF NOT EXISTS `{}`'.format(db))
    log.info(sql)

    # We don't care if the db already exists and this was a no-op
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    cursor.execute(sql)
    warnings.resetwarnings()


def drop_db(instance, db):
    """ Drop a database, if it exists.

    Args:
    instance - a hostAddr object
    db - the name of the db to be dropped
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ('DROP DATABASE IF EXISTS `{}`'.format(db))
    log.info(sql)

    # If the DB isn't there, we'd throw a warning, but we don't
    # actually care; this will be a no-op.
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    cursor.execute(sql)
    warnings.resetwarnings()


def copy_db_schema(instance, old_db, new_db, verbose=False, dry_run=False):
    """ Copy the schema of one db into a different db

    Args:
    instance - a hostAddr object
    old_db - the source of the schema copy
    new_db - the destination of the schema copy
    verbose - print out SQL commands
    dry_run - do not change any state
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    tables = get_tables(instance, old_db)
    for table in tables:
        raw_sql = "CREATE TABLE IF NOT EXISTS `{new_db}`.`{table}` LIKE `{old_db}`.`{table}`"
        sql = raw_sql.format(old_db=old_db, new_db=new_db, table=table)
        if verbose:
            print sql

        if not dry_run:
            cursor.execute(sql)


def move_db_contents(instance, old_db, new_db, dry_run=False):
    """ Move the contents of one db into a different db

    Args:
    instance - a hostAddr object
    old_db - the source from which to move data
    new_db - the destination to move data
    verbose - print out SQL commands
    dry_run - do not change any state
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    tables = get_tables(instance, old_db)
    for table in tables:
        raw_sql = "RENAME TABLE `{old_db}`.`{table}` to `{new_db}`.`{table}`"
        sql = raw_sql.format(old_db=old_db, new_db=new_db, table=table)
        log.info(sql)

        if not dry_run:
            cursor.execute(sql)


def start_event_scheduler(instance):
    """ Enable the event scheduler on a given MySQL server.

    Args:
        instance: The hostAddr object to act upon.
    """
    cmd = 'SET GLOBAL event_scheduler=ON'

    # We don't use the event scheduler in many places, but if we're
    # not able to start it when we want to, that could be a big deal.
    try:
        conn = connect_mysql(instance)
        cursor = conn.cursor()
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        log.info(cmd)
        cursor.execute(cmd)
    except Exception as e:
        log.error('Unable to start event scheduler: {}'.format(e))
        raise
    finally:
        warnings.resetwarnings()


def stop_event_scheduler(instance):
    """ Disable the event scheduler on a given MySQL server.

    Args:
        instance: The hostAddr object to act upon.
    """
    cmd = 'SET GLOBAL event_scheduler=OFF'

    # If for some reason we're unable to disable the event scheduler,
    # that isn't likely to be a big deal, so we'll just pass after
    # logging the exception.
    try:
        conn = connect_mysql(instance)
        cursor = conn.cursor()
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        log.info(cmd)
        cursor.execute(cmd)
    except Exception as e:
        log.error('Unable to stop event scheduler: {}'.format(e))
        pass
    finally:
        warnings.resetwarnings()


def setup_replication(new_main, new_replica, auto_pos=True):
    """ Set an instance as a subordinate of another

    Args:
    new_main - A hostaddr object for the new main
    new_subordinate - A hostaddr object for the new subordinate
    auto_pos - Do we want GTID auto positioning (if GTID is enabled)?
    """
    log.info('Setting {new_replica} as a replica of new main '
             '{new_main}'.format(new_main=new_main,
                                   new_replica=new_replica))

    new_main_coordinates = get_main_status(new_main)
    change_main(new_replica, new_main,
                  new_main_coordinates['File'],
                  new_main_coordinates['Position'],
                  gtid_auto_pos=auto_pos)


def restart_replication(instance):
    """ Stop then start replication

    Args:
    instance - A hostAddr object
    """
    stop_replication(instance)
    start_replication(instance)


def stop_replication(instance, thread_type=REPLICATION_THREAD_ALL):
    """ Stop replication, if running

    Args:
    instance - A hostAddr object
    thread - Which thread to stop. Options are in REPLICATION_THREAD_TYPES.
    """
    if thread_type not in REPLICATION_THREAD_TYPES:
        raise Exception('Invalid input for arg thread: {thread}'
                        ''.format(thread=thread_type))

    conn = connect_mysql(instance)
    cursor = conn.cursor()

    ss = get_subordinate_status(instance)
    if (ss['Subordinate_IO_Running'] != 'No' and ss['Subordinate_SQL_Running'] != 'No' and
            thread_type == REPLICATION_THREAD_ALL):
        cmd = 'STOP SLAVE'
    elif ss['Subordinate_IO_Running'] != 'No' and thread_type != REPLICATION_THREAD_SQL:
        cmd = 'STOP SLAVE IO_THREAD'
    elif ss['Subordinate_SQL_Running'] != 'No' and thread_type != REPLICATION_THREAD_IO:
        cmd = 'STOP SLAVE SQL_THREAD'
    else:
        log.info('Replication already stopped')
        return

    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    log.info(cmd)
    cursor.execute(cmd)
    warnings.resetwarnings()


def start_replication(instance, thread_type=REPLICATION_THREAD_ALL):
    """ Start replication, if not running

    Args:
    instance - A hostAddr object
    thread - Which thread to start. Options are in REPLICATION_THREAD_TYPES.
    """
    if thread_type not in REPLICATION_THREAD_TYPES:
        raise Exception('Invalid input for arg thread: {thread}'
                        ''.format(thread=thread_type))

    conn = connect_mysql(instance)
    cursor = conn.cursor()

    ss = get_subordinate_status(instance)
    if (ss['Subordinate_IO_Running'] != 'Yes' and ss['Subordinate_SQL_Running'] != 'Yes' and
            thread_type == REPLICATION_THREAD_ALL):
        cmd = 'START SLAVE'
    elif ss['Subordinate_IO_Running'] != 'Yes' and thread_type != REPLICATION_THREAD_SQL:
        cmd = 'START SLAVE IO_THREAD'
    elif ss['Subordinate_SQL_Running'] != 'Yes' and thread_type != REPLICATION_THREAD_IO:
        cmd = 'START SLAVE SQL_THREAD'
    else:
        log.info('Replication already running')
        return

    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    log.info(cmd)
    cursor.execute(cmd)
    warnings.resetwarnings()
    time.sleep(1)


def reset_subordinate(instance):
    """ Stop replicaion and remove all repl settings

    Args:
    instance - A hostAddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    try:
        stop_replication(instance)
        cmd = 'RESET SLAVE ALL'
        log.info(cmd)
        cursor.execute(cmd)
    except ReplicationError:
        # SHOW SLAVE STATUS failed, previous state does not matter so pass
        pass


def reset_main(instance):
    """ Clear the binlogs and any GTID information that may exist.
        We may have to do this when setting up a new subordinate.

    Args:
    instance - A hostAddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    cmd = 'RESET MASTER'
    log.info(cmd)
    cursor.execute(cmd)
    conn.close()


def change_main(subordinate_hostaddr, main_hostaddr, main_log_file,
                  main_log_pos, no_start=False, skip_set_readonly=False,
                  gtid_purged=None, gtid_auto_pos=True):
    """ Setup MySQL replication on new replica

    Args:
    subordinate_hostaddr -  hostaddr object for the new replica
    hostaddr - A hostaddr object for the main db
    main_log_file - Replication log file to begin streaming
    main_log_pos - Position in main_log_file
    no_start - If set, don't run START SLAVE after CHANGE MASTER
    skip_set_readonly - If set, don't set read-only flag
    gtid_purged - A set of GTIDs that we have already applied,
                  if we're in GTID mode.
    gtid_auto_pos - If set, use GTID auto-positioning if we're also
                    in GTID mode.
    """
    conn = connect_mysql(subordinate_hostaddr)
    cursor = conn.cursor()

    if not skip_set_readonly:
        set_global_variable(subordinate_hostaddr, 'read_only', True)

    reset_subordinate(subordinate_hostaddr)
    main_user, main_password = get_mysql_user_for_role('replication')

    parameters = {'main_user': main_user,
                  'main_password': main_password,
                  'main_host': main_hostaddr.hostname,
                  'main_port': main_hostaddr.port,
                  'main_log_file': main_log_file,
                  'main_log_pos': main_log_pos}

    sql_base = ("CHANGE MASTER TO "
                "MASTER_USER=%(main_user)s, "
                "MASTER_PASSWORD=%(main_password)s, "
                "MASTER_PORT=%(main_port)s, "
                "MASTER_HOST=%(main_host)s, "
                "{extra}")

    # are we in a GTID-based cluster - i.e., both main and subordinate
    # have GTID enabled?  If so, we need a slightly different SQL
    # statement.
    main_globals = get_global_variables(main_hostaddr)
    subordinate_globals = get_global_variables(subordinate_hostaddr)
    if main_globals['version'] >= '5.6' and subordinate_globals['version'] >= '5.6':
        use_gtid = main_globals['gtid_mode'] == 'ON' and \
                   subordinate_globals['gtid_mode'] == 'ON'
    else:
        # GTID not supported at all.
        use_gtid = None

    # if GTID is enabled but gtid_auto_pos is not set, we don't use
    # auto-positioning.  This will only happen in the case of a
    # shard migration / partial failover, which means that we're
    # taking a main and temporarily making it a subordinate.
    if use_gtid and gtid_auto_pos:
        sql = sql_base.format(extra='MASTER_AUTO_POSITION=1')
        del parameters['main_log_file']
        del parameters['main_log_pos']

        if gtid_purged:
            cursor.execute("SET GLOBAL gtid_purged=%(gtid_purged)s",
                           {'gtid_purged': gtid_purged})
            log.info(cursor._executed)
    else:
        extra = ("MASTER_LOG_FILE=%(main_log_file)s, "
                 "MASTER_LOG_POS=%(main_log_pos)s")
        if use_gtid is not None:
            extra = extra + ", MASTER_AUTO_POSITION=0"
        sql = sql_base.format(extra=extra)

    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    cursor.execute(sql, parameters)
    warnings.resetwarnings()
    log.info(cursor._executed)

    if not no_start:
        start_replication(subordinate_hostaddr)
        # Replication reporting is wonky for the first second
        time.sleep(1)
        # Avoid race conditions for zk update monitor
        # if we are all in Gtid's mode then we check both IO and SQL threads
        # after START SLAVE
        if gtid_auto_pos:
            assert_replication_sanity(subordinate_hostaddr,
                                  set([CHECK_SQL_THREAD, CHECK_IO_THREAD]))
        else:
            # But with gtid_migration set on where OLD main host not in
            # gtid mode yet we just don't check IO thread yet
            # Expecting IO thread will brake after failover before msyql get
            # restarted
            assert_replication_sanity(subordinate_hostaddr, set([CHECK_SQL_THREAD]))

def find_errant_trx(source, reference):
    """ If this is a GTID cluster, determine if this subordinate has any errant
        transactions.  If it does, we can't promote it until we fix them.

        Args:
            source: the source instance to check
            reference: the instance to compare to.  it may be a main,
                       or it may be another subordinate in the same cluster.
        Returns:
            A string of errant trx, or None if there aren't any.
    """
    ss = get_main_status(source).get('Executed_Gtid_Set').replace('\n', '')
    rs = get_main_status(reference).get('Executed_Gtid_Set').replace('\n', '')
    errant_trx = get_gtid_subtract(source, ss, rs)
    return errant_trx


def fix_errant_trx(gtids, target_instance, is_main=True):
    """ We have a set of GTIDs that don't belong.  Try to fix them.
        Args:
            gtids: A String of GTIDs eg:
            'bba7d5dd-fdfe-11e6-8b1e-12005f9b2b06:877821-877864, etc'
            target_instance: The hostaddr of the instance to use
            is_main: Is this a main?
        Returns:
            Nothing - either it works or it fails.
    """
    conn = connect_mysql(target_instance)
    cursor = conn.cursor()

    # not a main, we don't need to write these to the binlog.
    if not is_main:
        cursor.execute('SET SESSION sql_log_bin=0')

    sql = 'SET GTID_NEXT=%(gtid)s'
    # AFAIK, there is no 'bulk' way to do this.
    for gtid in gtids.split(','):
        #something we need to remove like '\nfa93a365-1307-11e7-944a-1204915fd524:1'
        (uuid, trx_list) = gtid.strip().split(':', 1)
        for trx in trx_list.split(':'):
            try:
                # it's a range.
                (start, end) = trx.split('-')
            except ValueError:
                # it's just a single transaction.
                (start, end) = (trx, trx)

            for i in xrange(int(start), int(end)+1):
                injected_trx = '{}:{}'.format(uuid, i)
                cursor.execute(sql, {'gtid': injected_trx})
                log.info(cursor._executed)
                conn.commit()
                cursor.execute('SET GTID_NEXT=AUTOMATIC')

    if not is_main:
        cursor.execute('SET SESSION sql_log_bin=1')
    conn.close()


def wait_for_catch_up(subordinate_hostaddr, io=False, migration=False):
    """ Watch replication or just the IO thread until it is caught up.
    The default is to watch replication overall.

    Args:
    subordinate_hostaddr - A HostAddr object
    io - if set, watch the IO thread only
    migration - if set, we're running a migration, so skip some of
                the expected-main sanity checks.
    """
    remaining_time = 'Not yet available'
    sleep_duration = 5
    last = None
    # Confirm that replication is even setup at all
    get_subordinate_status(subordinate_hostaddr)

    try:
        assert_replication_sanity(subordinate_hostaddr, migration=migration)
    except:
        log.warning('Replication does not appear sane, going to sleep 60 '
                    'seconds in case things get better on their own.')
        time.sleep(60)
        assert_replication_sanity(subordinate_hostaddr, migration=migration)

    invalid_lag = ('{} is unavailable, going to sleep for a minute and '
                   'retry. A likely reason is that there was a failover '
                   'between when a backup was taken and when a restore was '
                   'run so there will not be a entry until replication has '
                   'caught up more. If this is a new replica set, read_only '
                   'is probably ON on the main server.')
    acceptable_lag = ('{lag_type}: {current_lag} < {normal_lag}, '
                      'which is good enough.')
    eta_catchup_time = ('{lag_type}: {current_lag}.  Waiting for < '
                        '{normal_lag}.  Guestimate time to catch up: {eta}')

    if io:
        normal_lag = NORMAL_IO_LAG
        lag_type = 'IO thread lag (bytes)'
    else:
        normal_lag = NORMAL_HEARTBEAT_LAG - HEARTBEAT_SAFETY_MARGIN
        lag_type = 'Computed seconds behind main'

    while True:
        replication = calc_subordinate_lag(subordinate_hostaddr)
        lag = replication['io_bytes'] if io else replication['sbm']

        if lag is None or lag == INVALID:
            log.info(invalid_lag.format(lag_type))
            time.sleep(60)
            continue

        if lag < normal_lag:
            log.info(acceptable_lag.format(lag_type=lag_type,
                                           current_lag=lag,
                                           normal_lag=normal_lag))
            return

        # last is set at the end of the first execution and should always
        # be set from then on
        if last:
            catch_up_rate = (last - lag) / float(sleep_duration)
            if catch_up_rate > 0:
                remaining_time = datetime.timedelta(
                    seconds=((lag - normal_lag) / catch_up_rate))
                if remaining_time.total_seconds() > 6 * 60 * 60:
                    sleep_duration = 5 * 60
                elif remaining_time.total_seconds() > 60 * 60:
                    sleep_duration = 60
                else:
                    sleep_duration = 5
            else:
                remaining_time = '> heat death of the universe'
                sleep_duration = 60
        else:
            remaining_time = 'Not yet available'

        log.info(eta_catchup_time.format(lag_type=lag_type,
                                         current_lag=lag,
                                         normal_lag=normal_lag,
                                         eta=str(remaining_time)))

        last = lag
        time.sleep(sleep_duration)


def assert_replication_unlagged(instance, lag_tolerance, dead_main=False):
    """ Confirm that replication lag is less than tolerance, otherwise
        throw an exception

    Args:
    instance - A hostAddr object of the replica
    lag_tolerance - Possibly values (constants):
                    'REPLICATION_TOLERANCE_NONE'- no lag is acceptable
                    'REPLICATION_TOLERANCE_NORMAL' - replica can be slightly lagged
                    'REPLICATION_TOLERANCE_LOOSE' - replica can be really lagged
    """
    # Test to see if the subordinate is setup for replication. If not, we are hosed
    replication = calc_subordinate_lag(instance, dead_main)
    problems = set()
    if lag_tolerance == REPLICATION_TOLERANCE_NONE:
        if replication['sql_bytes'] != 0:
            problems.add('Replica {r} is not fully synced, bytes behind: {b}'
                         ''.format(r=instance,
                                   b=replication['sql_bytes']))
    elif lag_tolerance == REPLICATION_TOLERANCE_NORMAL:
        if replication['sbm'] > NORMAL_HEARTBEAT_LAG:
            problems.add('Replica {r} has heartbeat lag {sbm} > {sbm_limit} seconds'
                         ''.format(sbm=replication['sbm'],
                                   sbm_limit=NORMAL_HEARTBEAT_LAG,
                                   r=instance))

        if replication['io_bytes'] > NORMAL_IO_LAG:
            problems.add('Replica {r} has IO lag {io_bytes} > {io_limit} bytes'
                         ''.format(io_bytes=replication['io_bytes'],
                                   io_limit=NORMAL_IO_LAG,
                                   r=instance))
    elif lag_tolerance == REPLICATION_TOLERANCE_LOOSE:
        if replication['sbm'] > LOOSE_HEARTBEAT_LAG:
            problems.add('Replica {r} has heartbeat lag {sbm} > {sbm_limit} seconds'
                         ''.format(sbm=replication['sbm'],
                                   sbm_limit=LOOSE_HEARTBEAT_LAG,
                                   r=instance))
    else:
        problems.add('Unkown lag_tolerance mode: {m}'.format(m=lag_tolerance))

    if problems:
        raise Exception(', '.join(problems))


def assert_replication_sanity(instance,
                              checks=ALL_REPLICATION_CHECKS,
                              migration=False):
    """ Confirm that a replica has replication running and from the correct
        source if the replica is in zk. If not, throw an exception.

    args:
    instance - A hostAddr object
    migration - Set this to true if we're running a migration so that some
                checks are skipped.
    checks - A set of checks to run.
    """
    problems = set()
    subordinate_status = get_subordinate_status(instance)
    if (CHECK_IO_THREAD in checks and
            subordinate_status['Subordinate_IO_Running'] != 'Yes'):
        problems.add('Replica {r} has IO thread not running'
                     ''.format(r=instance))

    if (CHECK_SQL_THREAD in checks and
            subordinate_status['Subordinate_SQL_Running'] != 'Yes'):
        problems.add('Replica {r} has SQL thread not running'
                     ''.format(r=instance))

    if CHECK_CORRECT_MASTER in checks and not migration:
        zk = host_utils.MysqlZookeeper()
        try:
            replica_set = zk.get_replica_set_from_instance(instance)
        except:
            # must not be in zk, returning
            return
        expected_main = zk.get_mysql_instance_from_replica_set(replica_set)
        actual_main = host_utils.HostAddr(':'.join((subordinate_status['Main_Host'],
                                                      str(subordinate_status['Main_Port']))))
        if expected_main != actual_main:
            problems.add('Main is {actual} rather than expected {expected} '
                         'for replica {r}'.format(actual=actual_main,
                                                  expected=expected_main,
                                                  r=instance))
    if problems:
        raise Exception(', '.join(problems))


def calc_subordinate_lag(subordinate_hostaddr, dead_main=False):
    """ Determine MySQL replication lag in bytes and binlogs

    Args:
    subordinate_hostaddr - A HostAddr object for a replica

    Returns:
    io_binlogs - Number of undownloaded binlogs. This is only slightly useful
                 as io_bytes spans binlogs. It mostly exists for dba amussement
    io_bytes - Bytes of undownloaded replication logs.
    sbm - Number of seconds of replication lag as determined by computing
          the difference between current time and what exists in a heartbeat
          table as populated by replication
    sql_binlogs - Number of unprocessed binlogs. This is only slightly useful
                  as sql_bytes spans binlogs. It mostly exists for dba
                  amussement
    sql_bytes - Bytes of unprocessed replication logs
    ss - None or the results of running "show subordinate status'
    """

    ret = {'sql_bytes': INVALID,
           'sql_binlogs': INVALID,
           'io_bytes': INVALID,
           'io_binlogs': INVALID,
           'sbm': INVALID,
           'ss': {'Subordinate_IO_Running': INVALID,
                  'Subordinate_SQL_Running': INVALID,
                  'Main_Host': INVALID,
                  'Main_Port': INVALID}}
    try:
        ss = get_subordinate_status(subordinate_hostaddr)
    except ReplicationError:
        # Not a subordinate, so return dict of INVALID
        return ret
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code == MYSQL_ERROR_CONN_HOST_ERROR:
            # Host down, but exists.
            return ret
        else:
            # Host does not exist or something else funky
            raise

    ret['ss'] = ss

    # if this is a GTID+auto-position subordinate, and we're running
    # this for the first time, we won't have values for this stuff.
    # so we'll make something up just so we can get started.
    if ss['Auto_Position'] and not ss['Relay_Main_Log_File']:
        subordinate_sql_pos = 1
        subordinate_io_pos = 1
        subordinate_sql_binlog_num = '000001'
        subordinate_io_binlog_num = '000001'
    else:
        subordinate_sql_pos = ss['Exec_Main_Log_Pos']
        subordinate_sql_binlog = ss['Relay_Main_Log_File']
        _, subordinate_sql_binlog_num = re.split('\.', subordinate_sql_binlog)
        subordinate_io_pos = ss['Read_Main_Log_Pos']
        subordinate_io_binlog = ss['Main_Log_File']
        _, subordinate_io_binlog_num = re.split('\.', subordinate_io_binlog)

    main_hostaddr = host_utils.HostAddr(':'.join((ss['Main_Host'],
                                                    str(ss['Main_Port']))))
    if not dead_main:
        try:
            main_logs = get_main_logs(main_hostaddr)

            (ret['sql_bytes'], ret['sql_binlogs']) = calc_binlog_behind(subordinate_sql_binlog_num,
                                                                        subordinate_sql_pos,
                                                                        main_logs)
            (ret['io_bytes'], ret['io_binlogs']) = calc_binlog_behind(subordinate_io_binlog_num,
                                                                      subordinate_io_pos,
                                                                      main_logs)
        except _mysql_exceptions.OperationalError as detail:
            (error_code, msg) = detail.args
            if error_code != MYSQL_ERROR_CONN_HOST_ERROR:
                raise
            # we can compute real lag because the main is dead

    try:
        ret['sbm'] = calc_alt_sbm(subordinate_hostaddr, ss['Main_Server_Id'])
    except MySQLdb.ProgrammingError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_NO_SUCH_TABLE:
            raise
        # We can not compute a real sbm, so the caller will get
        # None default
        pass
    return ret


def calc_alt_sbm(instance, main_server_id):
    """ Calculate seconds behind using heartbeat + time on subordinate server

    Args:
    instance - A hostAddr object of a subordinate server
    main_server_id - The server_id of the main server

    Returns:
    An int of the calculated seconds behind main or None
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ''.join(("SELECT TIMESTAMPDIFF(SECOND,ts, NOW()) AS 'sbm' "
                   "FROM {METADATA_DB}.heartbeat "
                   "WHERE server_id= %(Main_Server_Id)s"))

    cursor.execute(sql.format(METADATA_DB=METADATA_DB),
                   {'Main_Server_Id': main_server_id})
    row = cursor.fetchone()
    if row:
        return row['sbm']
    else:
        return None


def get_heartbeat(instance):
    """ Get the most recent heartbeat on a subordinate

    Args:
    instance - A hostAddr object of a subordinate server

    Returns:
    A datetime.datetime object.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    subordinate_status = get_subordinate_status(instance)
    sql = ''.join(("SELECT ts "
                   "FROM {METADATA_DB}.heartbeat "
                   "WHERE server_id= %(Main_Server_Id)s"))

    cursor.execute(sql.format(METADATA_DB=METADATA_DB), subordinate_status)
    row = cursor.fetchone()
    if not row:
        return None

    return datetime.datetime.strptime(row['ts'], MYSQL_DATETIME_TO_PYTHON)


def get_pitr_data(instance):
    """ Get all data needed to run a point in time recovery later on

    Args:
    instance - A hostAddr object of a server

    Returns:
    """
    ret = dict()
    ret['heartbeat'] = str(get_heartbeat(instance))
    ret['repl_positions'] = []
    main_status = get_main_status(instance)
    ret['repl_positions'].append((main_status['File'], main_status['Position']))
    if 'Executed_Gtid_Set' in main_status:
        ret['Executed_Gtid_Set'] = main_status['Executed_Gtid_Set']
    else:
        ret['Executed_Gtid_Set'] = None

    try:
        ss = get_subordinate_status(instance)
        ret['repl_positions'].append((ss['Relay_Main_Log_File'], ss['Exec_Main_Log_Pos']))
    except ReplicationError:
        # we are running on a main, don't care about this exception
        pass

    return ret


def set_global_variable(instance, variable, value, check_existence=False):
    """ Modify MySQL global variables

    Args:
    instance - a hostAddr object
    variable - a string the MySQL global variable name
    value - a string or bool of the deisred state of the variable
    check_existence - verify that the desired variable exists before
                      we try to set it.  if the existence check fails,
                      we output a warning message and return False.
                      Most of the time we don't care, but for things
                      that are version-specific, we might.
    Returns:
        True or False
    """

    conn = connect_mysql(instance)
    cursor = conn.cursor()
    gvars = get_global_variables(instance)

    if check_existence and variable not in gvars:
        log.warning("{} is not a valid variable name for this MySQL "
                    "instance".format(variable))
        return False

    # If we are enabling read only we need to kill all long running trx
    # so that they don't block the change
    if variable == 'read_only' and value:
        if gvars[variable] == 'ON':
            log.debug('read_only is already enabled.')
            # no use trying to set something that is already turned on
            return True
        else:
            kill_long_trx(instance)

    parameters = {'value': value}
    # Variable is not a string and can not be paramaretized as per normal
    sql = 'SET GLOBAL {} = %(value)s'.format(variable)
    cursor.execute(sql, parameters)
    log.info(cursor._executed)
    return True


def start_consistent_snapshot(conn, read_only=False, session_id=None):
    """ Start a transaction with a consistent view of data

    Args:
        instance - a hostAddr object
        read_only - set the transaction to be read_only
        session_id - a MySQL session ID to base the snapshot on
    """
    read_write_mode = 'READ ONLY' if read_only else 'READ WRITE'
    session = 'FROM SESSION {}'.format(session_id) if session_id else ''
    cursor = conn.cursor()
    cursor.execute("SET SESSION TRANSACTION ISOLATION "
                   "LEVEL REPEATABLE READ")
    cursor.execute("START TRANSACTION /*!50625 WITH CONSISTENT SNAPSHOT "
                   "{s}, {rwm} */".format(s=session, rwm=read_write_mode))


def get_long_trx(instance):
    """ Get the thread id's of long (over 2 sec) running transactions

    Args:
    instance - a hostAddr object

    Returns -  A set of thread_id's
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ('SELECT trx_mysql_thread_id '
           'FROM information_schema.INNODB_TRX '
           'WHERE trx_started < NOW() - INTERVAL 2 SECOND ')
    cursor.execute(sql)
    transactions = cursor.fetchall()
    threads = set()
    for trx in transactions:
        threads.add(trx['trx_mysql_thread_id'])

    return threads


def kill_user_queries(instance, username):
    """ Kill a users queries

    Args:
    instance - The instance on which to kill the queries
    username - The name of the user to kill
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    sql = ("SELECT id "
           "FROM information_schema.processlist "
           "WHERE user=%(username)s ")
    cursor.execute(sql, {'username': username})
    queries = cursor.fetchall()
    for query in queries:
        log.info("Killing connection id {id}".format(id=query['id']))
        cursor.execute("kill %(id)s", {'id': query['id']})


def kill_long_trx(instance):
    """ Kill long running transaction.

    Args:
    instance - a hostAddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    threads_to_kill = get_long_trx(instance)
    for thread in threads_to_kill:
        try:
            sql = 'kill %(thread)s'
            cursor.execute(sql, {'thread': thread})
            log.info(cursor._executed)
        except MySQLdb.OperationalError as detail:
            (error_code, msg) = detail.args
            if error_code != MYSQL_ERROR_NO_SUCH_THREAD:
                raise
            else:
                log.info('Thread {thr} no longer '
                         'exists'.format(thr=thread))

    log.info('Confirming that long running transactions have gone away')
    while True:
        long_threads = get_long_trx(instance)
        not_dead = threads_to_kill.intersection(long_threads)

        if not_dead:
            log.info('Threads of not dead yet: '
                     '{threads}'.format(threads=not_dead))
            time.sleep(.5)
        else:
            log.info('All long trx are now dead')
            return


def shutdown_mysql(instance):
    """ Send a mysqladmin shutdown to an instance

    Args:
    instance - a hostaddr object
    """
    username, password = get_mysql_user_for_role('admin')
    cmd = ''.join((MYSQLADMIN,
                   ' -u ', username,
                   ' -p', password,
                   ' -h ', instance.hostname,
                   ' -P ', str(instance.port),
                   ' shutdown'))
    log.info(cmd)
    host_utils.shell_exec(cmd)


def get_mysqlops_connections():
    """ Get a connection to mysqlops for reporting

    Returns:
    A mysql connection
    """
    (reporting_host, port, _, _) = mysql_connect.get_mysql_connection('mysqlopsdb001')
    reporting = host_utils.HostAddr(''.join((reporting_host, ':', str(port))))
    return connect_mysql(reporting, 'dbascript')


def start_backup_log(instance, backup_type, timestamp):
    """ Start a log of a mysql backup

    Args:
    instance - A hostaddr object for the instance being backed up
    backup_type - Either xbstream or sql
    timestamp - The timestamp from when the backup began
    """
    row_id = None
    try:
        reporting_conn = get_mysqlops_connections()
        cursor = reporting_conn.cursor()

        sql = ("INSERT INTO mysqlops.mysql_backups "
               "SET "
               "hostname = %(hostname)s, "
               "port = %(port)s, "
               "started = %(started)s, "
               "backup_type = %(backup_type)s ")

        metadata = {'hostname': instance.hostname,
                    'port': str(instance.port),
                    'started': time.strftime('%Y-%m-%d %H:%M:%S', timestamp),
                    'backup_type': backup_type}
        cursor.execute(sql, metadata)
        row_id = cursor.lastrowid
        reporting_conn.commit()
        log.info(cursor._executed)
    except Exception as e:
        log.warning("Unable to write log entry to "
                    "mysqlopsdb001: {e}".format(e=e))
        log.warning("However, we will attempt to continue with the backup.")
    return row_id


def finalize_backup_log(id, filename):
    """ Write final details of a mysql backup

    id - A pk from the mysql_backups table
    filename - The location of the resulting backup
    """
    try:
        reporting_conn = get_mysqlops_connections()
        cursor = reporting_conn.cursor()
        sql = ("UPDATE mysqlops.mysql_backups "
               "SET "
               "filename = %(filename)s, "
               "finished = %(finished)s "
               "WHERE id = %(id)s")
        metadata = {'filename': filename,
                    'finished': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'id': id}
        cursor.execute(sql, metadata)
        reporting_conn.commit()
        reporting_conn.close()
        log.info(cursor._executed)
    except Exception as e:
        log.warning("Unable to update mysqlopsdb with "
                    "backup status: {e}".format(e=e))


def get_installed_mysqld_version():
    """ Get the version of mysqld installed on localhost

    Returns the numeric MySQL version

    Example: 5.6.22-72.0
    """
    (std_out, std_err, return_code) = host_utils.shell_exec(MYSQL_VERSION_COMMAND)
    if return_code or not std_out:
        raise Exception('Could not determine installed mysql version: '
                        '{std_err}')
    return re.search('.+Ver ([0-9.-]+)', std_out).groups()[0]


def get_autoincrement_type(instance, db, table):
    """ Get type of autoincrement field

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db
    table - the name of the table to fetch columns

    Returns:
        str: autoincrement column type
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    param = {'db': db,
             'table': table}

    # Get the max value of the autoincrement field
    type_query = ("SELECT COLUMN_TYPE "
                  "FROM INFORMATION_SCHEMA.columns "
                  "WHERE TABLE_NAME=%(table)s AND "
                  "      TABLE_SCHEMA=%(db)s AND "
                  "      EXTRA LIKE '%%auto_increment%%';")
    cursor.execute(type_query, param)
    ai_type = cursor.fetchone()

    cursor.close()
    conn.close()

    if ai_type and 'COLUMN_TYPE' in ai_type:
        return ai_type['COLUMN_TYPE']

    return None


def get_autoincrement_value(instance, db, table):
    """ Get the current value of the autoincrement field

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db
    table - the name of the table to fetch columns

    Returns:
        long: next value of the autoincrement field
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    param = {'db': db,
             'table': table}

    # Get the size of the autoincrement field
    value_query = ("SELECT AUTO_INCREMENT "
                   "FROM INFORMATION_SCHEMA.TABLES "
                   "WHERE TABLE_NAME=%(table)s AND "
                   "      TABLE_SCHEMA=%(db)s;")
    cursor.execute(value_query, param)
    ai_value = cursor.fetchone()

    cursor.close()
    conn.close()

    if ai_value and 'AUTO_INCREMENT' in ai_value:
        return ai_value['AUTO_INCREMENT']
    return None


def get_autoincrement_info_by_db(instance, db):
    """ Gets the current values of the autoincrement fields on a db basis

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db

    Returns:
        (dict of str: tuple):
            table_name:(autoincrement size,
                        the most recent value of the autoincrement field)
    """

    tables = get_tables(instance, db)
    ret = {}
    for table in tables:
        ai_val = get_autoincrement_value(instance, db, table)
        ai_type = get_autoincrement_type(instance, db, table)
        ret[table] = (ai_type, ai_val)
    return ret


def get_approx_schema_size(instance, db):
    """ Get the approximate size of a db in MB

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db

    Returns:
        (float) Size in MB
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ("SELECT SUM(data_length + index_length)/1048576 as 'mb' "
           "FROM information_schema.tables "
           "WHERE table_schema=%(db)s")
    cursor.execute(sql, {'db': db})

    size = cursor.fetchone()
    cursor.close()
    conn.close()
    return size['mb']


def get_row_estimate(instance, db, table):
    """ Get the approximate row count for a table.
        This could be wildly inaccurate.

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db
    table - the name of the table

    Returns:
        long: An estimated row count.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    analyze = ("ANALYZE TABLE {db}.{table}"
               "".format(db=db, table=table))
    cursor.execute(analyze)

    query = ("EXPLAIN SELECT COUNT(*) FROM {db}.{table}"
             "".format(db=db, table=table))
    cursor.execute(query)

    estimation = cursor.fetchone()
    cursor.close()
    conn.close()
    return estimation['rows']


def get_row_count(instance, db, table):
    """ Get the actual row count for a table

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db
    table - the name of the table

    Returns:
        long: An actual row count for the table
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    query = ("SELECT COUNT(*) AS 'rows' FROM {db}.{table}"
             "".format(db=db, table=table))
    cursor.execute(query)

    count = cursor.fetchone()
    cursor.close()
    conn.close()
    return count['rows']

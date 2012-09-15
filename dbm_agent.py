#!/usr/bin/env python
#
#
#
# omnidba@gmail.com

import fcntl
import logging
import os
import os.path
import re
import shutil
import socket
import stat
import sys
import tempfile
import threading

from optparse import OptionParser
from time import strftime, localtime, sleep

import dlpxqa

import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import TCPRegistryClient

try:
    import subprocess
    is_subprocess_available = True
except ImportError:
    is_subprocess_available = False

##############################################################################
# Main
##############################################################################
if not dlpxqa.is_run_as_root():
    sys.exit('\nERROR: must run this script as root.\n')

# parse command-line options
parser = OptionParser()
parser.add_option('-d', '--debug', action='store_true', dest='debug', default=False, help='debug mode')
parser.add_option('-r', '--registryserver', action='store', dest='regsrv_ip', help='registry server IP')

options, args = parser.parse_args()

if options.regsrv_ip:
    # get registry server IP from command-line option
    regsrv_ip = options.regsrv_ip
else:
    # get registry server IP from configuration file
    regsrv_ip = dlpxqa.get_regsrv_ip()

if options.debug:
    DEBUG = True
else:
    DEBUG = False

PSEFEGREP = dlpxqa.PS + '-ef | ' + dlpxqa.EGREP
WCL = dlpxqa.WC + '-l'
ORACLE_11gR2 = ('11.2.0.1.0', '11.2.0.2.0', '11.2.0.3.0')

def is_process_running(pid):
    process_count = PSEFEGREP + '\"' + pid + '\"' + ' | ' + dlpxqa.EGREP + '-v egrep | ' + WCL

    if DEBUG:
        print 'is_process_running(): process_count: ', process_count

    output = dlpxqa.exec_cmd(process_count)

    if int(output) != 0:
        return True
    else:
        return False

# get current path
current_dir = os.path.dirname(os.path.realpath(__file__))
if DEBUG:
    print 'Current directory is: %s' % current_dir

pid_file_path = os.path.join(current_dir, 'dbm_agent.pid')
if os.path.exists(pid_file_path):
    pid_file = open(pid_file_path, 'r')
    pid_string = pid_file.read()
    pid_file.close()
    if pid_string == os.getpid():
        sys.exit('ERROR: this script is supposed to run only once.')
    else:
        if is_process_running(pid_string):
            sys.exit('ERROR: this script is already running.')
        else:
            pid_file = open(pid_file_path, 'w')
            pid_file.write(str(os.getpid()))
            pid_file.close()
else:
    pid_file = open(pid_file_path, 'w')
    pid_file.write(str(os.getpid()))
    pid_file.close()

# define the DBMONITOR service
class DbmonitorService(rpyc.Service):
    def _run_sql(self, query, role, sid, asm_sid=''):
        """
        Runs SQL statement as specified role against specified SID.
        :param query: SQL statement
        :type query: str
        :param role: role
        :type role: str
        :param sid: SID
        :type sid: str
        :param asm_sid: SID of ASM instance
        :type asm_sid: str
        """
        if asm_sid == '':
            # if an SID for database instance is specified
            os.putenv('ORACLE_SID', sid)
            os.putenv('ORACLE_HOME', dlpxqa.get_db_home_by_sid(sid))
            SQLPLUS = os.path.join(dlpxqa.get_db_home_by_sid(sid), 'bin', 'sqlplus')
        else:
            # if an SID for ASM instance is specified
            # ORACLE_SID should set to ASM SID
            os.putenv('ORACLE_SID', asm_sid)

            cv = dlpxqa.get_crs_version()
            if DEBUG:
                print '_run_sql(): CRS version: ', cv

            if cv != '' and cv in ORACLE_11gR2:
                # for 11gR2, ASM home is GI home
                os.putenv('ORACLE_HOME', dlpxqa.get_crs_home())
                SQLPLUS = os.path.join(dlpxqa.get_crs_home(), 'bin', 'sqlplus')
            elif cv != '' and not cv in ORACLE_11gR2:
                # for 10gR2 and 11gR1, ASM home is Oracle home
                os.putenv('ORACLE_HOME', dlpxqa.get_db_home_by_sid(sid))
                SQLPLUS = os.path.join(dlpxqa.get_db_home_by_sid(sid), 'bin', 'sqlplus')
            else:
                os.putenv('ORACLE_HOME', dlpxqa.get_db_home_by_sid(sid))
                SQLPLUS = os.path.join(dlpxqa.get_db_home_by_sid(sid), 'bin', 'sqlplus')

        if DEBUG:
            print '_run_sql(): SQLPLUS: ', SQLPLUS
            print '_run_sql(): query: ', query
            print '_run_sql(): SID: ', sid
            print '_run_sql(): ASM SID: ', asm_sid

        QUERY_HEADER = 'set linesize 9000\nset pagesize 9999\nset newpage none\nset feedback off\nset verify off\nset echo off\nset heading off\n'
        QUERY_END = '\nquit;'

        # try to catch OSError and propagate it to to higher handler
        try:
            temp_dir = tempfile.mkdtemp()
        except OSError:
            raise

        # add read and execute permissions for others
        mode = os.stat(temp_dir).st_mode
        os.chmod(temp_dir, mode | stat.S_IROTH | stat.S_IXOTH)

        sql_filename = os.path.join(temp_dir, '%s.sql' % os.getpid())
        sql_file = open(sql_filename, 'w+b')

        try:
            sql_file.write(QUERY_HEADER)
            sql_file.write(query)
            sql_file.write(QUERY_END)
        finally:
            sql_file.close()

        os_login = dlpxqa.get_database_os_login(sid)
        os_user = re.search(r'^(\S+)\/', os_login).group(1)

        if DEBUG:
            print '_run_sql(): os_user: ', os_user

        sqlplus_cmd = SQLPLUS + ' -s \"' + '/ as ' + role + '\"' + ' @' + sql_file.name
        sqlplus_cmd = dlpxqa.SU + os_user + ' -c \'' + sqlplus_cmd + '\''

        if DEBUG:
            print '_run_sql(): sqlplus_cmd: ', sqlplus_cmd

        output = dlpxqa.exec_cmd(sqlplus_cmd)

        shutil.rmtree(temp_dir)
        return output

    def exposed_get_host_name(self):
        hostname = socket.gethostname()
        return hostname

    def exposed_is_process_running(self, process):
        process_count = PSEFEGREP + '\"' + process + '\"' + ' | ' + dlpxqa.EGREP + '-v egrep | ' + WCL

        if DEBUG:
            print 'exposed_is_process_running(): process_count: ', process_count

        output = dlpxqa.exec_cmd(process_count)

        if int(output) != 0:
            return True
        else:
            return False

    def exposed_get_database_role(self, sid):
        try:
            output = self._run_sql('select database_role from v$database;', 'sysdba', sid)
        except Exception:
            raise
        database_role = output
        return database_role

    def exposed_get_archive_dest(self, sid):
        """
        Returns database's archive log destination.
        :param sid: SID
        :type sid: str
        :rtype: str 
        """
        archive_dest = ''
        try:
            output = self._run_sql('archive log list;', 'sysdba', sid)
        except Exception:
            raise

        if DEBUG:
            print 'exposed_get_archive_dest():\n', output

        for line in output.split('\n'):
            m = re.search(r'Archive destination\t\s+(\S+)', line)
            if m:
                archive_dest = m.group(1)
                break
        return archive_dest

    def exposed_get_archive_dest_usage(self, database):
        """
        Returns database's archive log destination's usage.
        :param database: database
        :type database: str
        :rtype: str
        """
        dest_type = ''
        dest_usage = ''

        try:
            archive_dest = self.exposed_get_archive_dest(database)
        except Exception:
            raise

        if re.search(r'\+\S+', archive_dest):
            # archive destination is an ASM diskgroup
            dest_type = 'ASM'
            # remove prefix '+'
            diskgroup_name = re.sub(r'\+', '', archive_dest)
            # get SID for ASM instance
            asm_sid = dlpxqa.get_dsource_asm_sid(database)
            if DEBUG:
                print 'exposed_get_archive_dest_usage():\n', asm_sid

            try:
                dest_usage = self._run_sql('select name,total_mb,free_mb from v$asm_diskgroup where name=\'%s\';' % diskgroup_name, 'sysdba', database, asm_sid)
            except Exception:
                raise

            if DEBUG:
                print 'exposed_get_archive_dest_usage():\n', dest_usage
        elif re.search(r'\/\S+', archive_dest):
            # archive destination is on filesystem
            dest_type = 'Filesystem'
            cmd = dlpxqa.DF + '-h ' + archive_dest
            dest_usage = dlpxqa.exec_cmd(cmd)
            if DEBUG:
                print 'exposed_get_archive_dest_usage():\n', dest_usage
        else:
            sys.exit('ERROR: un-recognized archive destination.')

        return dest_type, dest_usage

# start the DB Monitor server 
dbmsrv = ThreadedServer(DbmonitorService, registrar=TCPRegistryClient(regsrv_ip))
dbmsrv.start()

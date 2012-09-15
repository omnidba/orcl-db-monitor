#!/usr/bin/env python
#
#
#
# omnidba@gmail.com

import ConfigParser
import logging
import os
import os.path
import re
import socket
import subprocess
import sys
import threading

from optparse import OptionParser
from time import strftime, localtime

import dlpxqa
import dbm_connection

#logger = dlpxqa.get_file_logger('DBMONITOR')

##############################################################################
# Functions
##############################################################################
def get_connections(connecion_factory, service_name, databases, registrar_ip):
    """
    Returns a dictionary of IP:connection.
    :param connection_factory: connection factory
    :param service_name: service name
    :type service_name: str
    :param databases: a list of databases
    :type databases: list
    :param registrar_ip: IP address of registrar server
    :type registrar_ip: str
    """
    connection_dict = {}

    for database in databases:
        host_name = dlpxqa.get_host_name_by_database(database)
        connection = connection_factory.create(service_name, database, host_name, registrar_ip)
        connection_dict[connection.ip] = connection

    if DEBUG:
        print "get_connections(): connection_dict:\n", connection_dict

    return connection_dict

##############################################################################
# Main
##############################################################################
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

connection_factory = dbm_connection.ConnectionFactory()

print strftime("%Y-%m-%d %H:%M:%S", localtime())

conn_dict = get_connections(connection_factory, 'DBMONITOR', dlpxqa.get_database_list(), regsrv_ip)
discovery_failure_host_dict = {}
connection_failure_host_dict = {}

for conn in conn_dict.values():
    if conn.is_connected():
        print (70 * '=')
        host_name = conn.root.get_host_name()
        sid = conn.database
        ip = dlpxqa.get_database_ip(sid)
        login = dlpxqa.get_database_os_login(sid)
        print 'HOST : %s (%s)' % (host_name, ip)
        print 'SID  : %s' % sid
        print 'LOGIN: %s' % login

        try:
            database_role = conn.root.get_database_role(sid)
        except OSError, e:
            print 'ERROR: OS error raised: %s' % e.strerror
            continue
        print 'ROLE : %s' % database_role.rstrip()

        archive_dest = conn.root.get_archive_dest(sid)
        print 'ARCHIVE DEST     : %s' % archive_dest

        type, usage = conn.root.get_archive_dest_usage(sid)
        print 'ARCHIVE DEST TYPE: %s' % type
        if DEBUG:
            print 'ARCHIVE DEST USAGE:\n%s' % usage
        if type == 'ASM':
            print 'FREE SPACE (MB)  : %s' % (usage.split()[2])
        else: # type == 'Filesystem'
            for line in usage.splitlines():
                if re.search(r'\d+%', line):
                    if DEBUG:
                        print 'line: %s' % line
                    print 'FREE SPACE       : %s' % line.split()[2]
    else: # conn.is_connected() == False
        if not conn.is_service_discovered():
            discovery_failure_host_dict[conn.host_name] = conn.ip
        else: # conn.is_service_discoverd() == True
            connection_failure_host_dict[conn.host_name] = conn.ip

print (70 * '=')
print 'ERROR: cannot discover service DBMONITOR at host(s):'
for host in discovery_failure_host_dict.keys():
    print (70 * '-')
    print 'HOST : %s (%s)' % (host, discovery_failure_host_dict[host])

print (70 * '=')
print 'ERROR: cannot connect to service DBMONITOR at host(s):'
for host in connection_failure_host_dict.keys():
    print (70 * '-')
    print 'HOST : %s (%s)' % (host, connection_failure_host_dict[host])

print (70 * '=')
print strftime("%Y-%m-%d %H:%M:%S", localtime())

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

from time import strftime, localtime

try:
    import subprocess
    is_subprocess_available = True
except ImportError:
    is_subprocess_available = False

# get current path
current_dir = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = os.path.join(current_dir, 'config.ini')
DATABASE_FILE = os.path.join(current_dir, 'database.ini')
DEBUG = False

sysname = os.uname()[0]
if sysname == 'SunOS':
    orainst_loc = '/var/opt/oracle/oraInst.loc'
    oratab_loc = '/var/opt/oracle/oratab'

    CAT = '/usr/bin/cat '
    CRONTAB = '/usr/bin/crontab '
    CUT = '/usr/bin/cut '
    DF = '/usr/bin/df '
    EGREP = '/usr/bin/egrep '
    FIND = '/usr/bin/find '
    GREP = '/usr/bin/grep '
    KILL = '/usr/bin/kill '
    NOHUP = '/usr/bin/nohup '
    IFCONFIG = '/usr/sbin/ifconfig '
    PS = '/bin/ps '
    RM = '/usr/bin/rm '
    SU = '/usr/bin/su '
    WC = 'bin/wc '
    XARGS = '/usr/bin/xargs '
elif sysname == 'Linux':
    orainst_loc = '/etc/oraInst.loc'
    oratab_loc = '/etc/oratab'

    CAT = '/bin/cat '
    CRONTAB = '/usr/bin/crontab '
    CUT = '/bin/cut '
    DF = '/bin/df '
    EGREP = '/bin/egrep '
    FIND = '/usr/bin/find '
    GREP = '/bin/grep '
    KILL = '/bin/kill '
    NOHUP = '/usr/bin/nohup '
    IFCONFIG = '/sbin/ifconfig '
    PS = '/bin/ps '
    RM = '/bin/rm '
    SU = '/bin/su '
    WC = '/usr/bin/wc '
    XARGS = '/usr/bin/xargs '
elif sysname == 'AIX':
    orainst_loc = '/etc/oraInst.loc'
    oratab_loc = '/etc/oratab'

    CAT = '/usr/bin/cat '
    CRONTAB = '/usr/bin/crontab '
    CUT = '/usr/bin/cut '
    DF = '/usr/bin/df '
    EGREP = '/usr/bin/egrep '
    FIND = '/usr/bin/find '
    GREP = '/usr/bin/grep '
    KILL = '/usr/bin/kill '
    NOHUP = '/usr/bin/nohup '
    IFCONFIG = '/etc/ifconfig '
    PS = '/usr/bin/ps '
    RM = '/usr/bin/rm '
    SU = '/usr/bin/su '
    WC = '/usr/bin/wc '
    XARGS = '/usr/bin/xargs '
elif sysname == 'HP-UX':
    orainst_loc = '/var/opt/oracle/oraInst.loc'
    oratab_loc = '/etc/oratab'

    CAT = '/bin/cat '
    CRONTAB = '/usr/bin/crontab '
    CUT = '/bin/cut '
    DF = '/usr/bin/bdf '
    EGREP = '/usr/bin/egrep '
    FIND = '/usr/bin/find '
    GREP = '/bin/grep '
    KILL = '/usr/bin/kill '
    NOHUP = '/usr/bin/nohup '
    IFCONFIG = '/usr/sbin/ifconfig '
    PS = '/usr/bin/ps '
    RM = '/usr/bin/rm '
    SU = '/usr/bin/su '
    WC = '/usr/bin/wc '
    XARGS = '/usr/bin/xargs '
else:
    sys.exit('ERROR: Unknown OS.')

def is_run_as_root():
    return os.geteuid() == 0

def get_crs_home():
    crs_home_loc = ''

    if os.path.isfile(orainst_loc):
        try:
            orainst_loc_file = open(orainst_loc, 'r')
            for line in orainst_loc_file:
                if DEBUG:
                    print 'ORAINST_LOC: %s' % line

                m = re.search(r'inventory_loc=(\S+)', line)
                if m:
                    inventory_loc = m.group(1)
                    break
        finally:
            orainst_loc_file.close()
    else:
        sys.exit('ERROR: oraInst.loc file does not exist.')

    if DEBUG:
        print 'INVENTORY_LOC: %s' % inventory_loc

    inventory_xml = os.path.join(inventory_loc, 'ContentsXML', 'inventory.xml')

    if os.path.isfile(inventory_xml):
        try:
            inventory_xml_file = open(inventory_xml, 'r')
            for line in inventory_xml_file:
                if DEBUG:
                    print 'INVENTORY_XML_FILE: %s' % line

                if re.search(r'CRS=\"true\"', line):
                    m = re.search(r'LOC=\"(\S+)\"', line)
                    if m:
                        crs_home_loc = m.group(1)
                    break
        finally:
            inventory_xml_file.close()
    else:
        sys.exit('ERROR: inventory.xml file does not exist.')

    if (crs_home_loc != '') and (not os.path.exists(crs_home_loc)):
        sys.exit('ERROR: CRS home %s does not exist on this host.' % crs_home_loc)
    return crs_home_loc

def get_crs_version():
    crs_version = ''

    if get_crs_home() != '':
        crsctl = os.path.join(get_crs_home(), 'bin', 'crsctl')
        cmd = crsctl + ' query crs activeversion'
        crs_version = exec_cmd(cmd)
        m = re.search(r'\[(\S+)\]$', crs_version)
        crs_version = m.group(1)
    return crs_version

def get_db_home_by_sid(sid):
    db_home_loc = ''

    # remove trailing number and/or underscore from sid 
    sid = re.search(r'([a-zA-Z]+)', sid).group(1)
    if DEBUG:
        print 'SID: %s' % sid

    if os.path.isfile(oratab_loc):
        try:
            oratab_loc_file = open(oratab_loc, 'r')
            for line in oratab_loc_file:
                if DEBUG:
                    print 'ORATAB_LOC: %s' % line

                m = re.search(r'%s:(\S+):' % sid, line)
                if m:
                    db_home_loc = m.group(1)
                    break
        finally:
            oratab_loc_file.close()
    else:
        sys.exit('ERROR: oratab file does not exist.')

    if (db_home_loc != '') and (not os.path.exists(db_home_loc)):
        sys.exit('ERROR: DB home %s for SID %s does not exist on this host.' % (db_home_loc, sid))
    return db_home_loc

def get_file_logger(logger_name, appliance):
    hostname = get_host_name(appliance)
    log_filename = re.match('(\w+)\.py', sys.argv[0]).group(1) + "_" + hostname + "_" + strftime("%Y%m%d%H%M%S", localtime()) + ".log"
    my_logger = logging.getLogger(logger_name)
    my_logger.setLevel(logging.INFO)

    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(name)s] [%(threadName)-10s] %(levelname)s %(message)s")
    fh.setFormatter(formatter)
    my_logger.addHandler(fh)

    print '%s log file location is:\n%s' % (logger_name, os.path.abspath(log_filename))
    return my_logger

def exec_cmd(cmd):
    if is_subprocess_available:
        output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()[0]
    else:
        output = os.popen(cmd, 'r').read()
    return output

def run_cmd(cmd):
    if is_subprocess_available:
        subprocess.Popen(cmd, shell=True)
    else:
        os.system(cmd)
    return

def exec_cli_cmd(appliance, cmd):
    logon_str = get_host_login(appliance) + '@' + get_host_ip(appliance) + ' '
    cli_path = get_cli_path()
    cmd = cli_path + ' ' + logon_str + cmd
    if is_subprocess_available:
        output = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE).communicate()[0]
    else:
        output = os.popen(cmd, 'r').read()
    return output

# get registry server IP from configuration file
def get_regsrv_ip():
    p = ConfigParser.ConfigParser()
    p.read(CONFIG_FILE)
    return p.get('regsrv', 'ip')

# get list of databases from database.ini
def get_database_list():
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.sections()

def get_host_name_by_database(section):
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.get(section, 'host_name')

def get_database_ip(section):
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.get(section, 'ip')

def get_database_os_login(section):
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.get(section, 'login')

def get_cli_path():
    p = ConfigParser.ConfigParser()
    p.read(CONFIG_FILE)
    return p.get('cli', 'path')

def get_host_dict():
    p = ConfigParser.ConfigParser()
    p.read(CONFIG_FILE)
    d = p.__dict__['_sections'].copy()
    d.pop('cli')
    d.pop('jwang65')
    d.pop('jwang79')
    return d

def get_dsource_sid(section):
    p = ConfigParser.ConfigParser()
    p.read(HOST_FILE)
    return p.get(section, 'sid')

def get_dsource_asm_sid(section):
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.get(section, 'asm_sid')

def get_dsource_db_user(section):
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.get(section, 'db_user')

def get_dsource_orahome(section):
    p = ConfigParser.ConfigParser()
    p.read(DATABASE_FILE)
    return p.get(section, 'orahome')

def get_toolkit_path(section, appliance):
    user = re.match('(\w+)\\/', get_host_login(section)).group(1)
    path = os.path.join('/home', user, get_host_name(appliance), 'delphix')
    return path

def test():
    print sysname
    print get_crs_home()
    print get_crs_version()
    print get_db_home_by_sid('chicago')
    print get_db_home_by_sid('chicago1')
    print get_db_home_by_sid('racp_1')
    print get_db_home_by_sid('RACDB1')
    print get_db_home_by_sid('nonexist')

if __name__ == '__main__':
    test()

#!/usr/bin/env python
#
#
#
# omnidba@gmail.com

import os
import os.path
import re
import shutil
import socket
import subprocess
import sys
import tempfile

from optparse import OptionParser
from time import strftime, localtime, sleep

import dlpxqa

PSEFEGREP = dlpxqa.PS + '-ef | ' + dlpxqa.EGREP
WCL = dlpxqa.WC + '-l'

##############################################################################
# Functions
##############################################################################
def get_dbm_agent_pid():
    pids = []

    processes = PSEFEGREP + '\"dbm_agent.py\"' + ' | ' + dlpxqa.EGREP + '-v egrep'
    process_count = processes + ' | ' + WCL

    if DEBUG:
        print 'get_dbm_agent_pid(): processes: ', processes
        print 'get_dbm_agent_pid(): process_count: ', process_count

    output = dlpxqa.exec_cmd(process_count)

    if DEBUG:
        print 'get_dbm_agent_pid(): count: ', output

    if int(output) != 0:
        lines = dlpxqa.exec_cmd(processes).split('\n')

        if DEBUG:
            print 'get_dbm_agent_pid(): processes: ', lines

        i = 0
        while lines[i] != '':
            pid = lines[i].split()[1]
            pids.append(pid)
            i = i + 1

    return pids

##############################################################################
# Main
##############################################################################
if not dlpxqa.is_run_as_root():
    sys.exit('\nERROR: must run this script as root.\n')

# parsing command-line options
parser = OptionParser()
parser.add_option('-d', '--debug', action='store_true', dest='debug', default=False, help='debug mode')
parser.add_option('-u', '--uninstall', action='store_true', dest='uninstall', default=False, help='uninstall crontab job')

options, args = parser.parse_args()

if options.debug:
    DEBUG = True
else:
    DEBUG = False

if options.uninstall:
    UNINST = True
else:
    UNINST = False

# get current path
current_dir = os.path.dirname(os.path.realpath(__file__))
if DEBUG:
    print 'Current directory is: %s' % current_dir

script_path = os.path.join(current_dir, 'dbm_agent.py')

# crontab entry for dbm_agent.py
dbm_agent_job = '*/5 * * * * /usr/bin/python ' + script_path + ' > /dev/null 2&>1'

# get current contents of crontab
cmd = dlpxqa.CRONTAB + '-l'
output = dlpxqa.exec_cmd(cmd)
jobs = output.split('\n')

print 'Current contents of crontab are:\n%s' % output

if DEBUG:
    print type(output), '\n', output, '\n', output.split('\n')

if not UNINST:
    print 'Following crontab job will be installed:\n%s' % dbm_agent_job
else:
    print 'Following crontab job will be uninstalled:\n%s' % dbm_agent_job

# create a temporary directory and file for storing new crontab     
temp_dir = tempfile.mkdtemp()
filename = os.path.join(temp_dir, '%s.txt' % os.getpid())
crontab_file = open(filename, 'a+b')

pattern = r'(^|\n).*dbm_agent\.py > \/dev\/null 2&>1'

if DEBUG:
    print '\nRE pattern is: %s\n' % pattern

if not UNINST:
    if not re.search(pattern, output):
        try:
            output.rstrip('\n')
            crontab_file.write(output)
            crontab_file.write(dbm_agent_job)
            crontab_file.write('\n')
        finally:
            crontab_file.close()
    else:
        sys.exit('\nERROR: this crontab job is already installed.\n')
else: # uninstall
    # check if dbm_agent.py is running, if yes, kill it first
    pids = get_dbm_agent_pid()
    if pids != []:
        for pid in pids:
            cmd = dlpxqa.KILL + ' -SIGKILL ' + pid
            dlpxqa.run_cmd(cmd)
    # substitute the dbm_agent.py job line with a blank line
    if re.search(pattern, output):
        try:
            output = re.sub(pattern, '', output)
            if DEBUG:
                print 'After substitution: %s' % output
            crontab_file.write(output)
        finally:
            crontab_file.close()
    else:
        sys.exit('\nERROR: there is no crontab job to uninstall.\n')

# install the new crontab
cmd = dlpxqa.CRONTAB + filename
if DEBUG:
    print cmd

dlpxqa.run_cmd(cmd)

# remove temporary directory  
shutil.rmtree(temp_dir)

# get current contents of crontab
cmd = dlpxqa.CRONTAB + '-l'
if DEBUG:
    print cmd

output = dlpxqa.exec_cmd(cmd)

print '\nCurrent contents of crontab are:\n%s' % output

print 'DONE.\n'

#!/usr/bin/env python
#
#
#
# omnidba@gmail.com

import socket

import dlpxqa

import rpyc
from rpyc.utils.factory import DiscoveryError
from rpyc.utils.registry import TCPRegistryClient

class ConnectionFactory(object):
    def create(self, service_name, database, host_name, registrar_ip):
        return Connection(service_name, database, host_name, registrar_ip)

class Connection(object):
    def __init__(self, service_name, database, host_name, registrar_ip):
        self._service_name = service_name
        self._database = database
        self._host_name = host_name
        self._ip = dlpxqa.get_database_ip(database)
        self._registrar = TCPRegistryClient(registrar_ip)
        self._addrs = []
        self._remote_host_ip = ''
        self._port = ''
        self._connection = None
        self._discovered_service = False
        self._connected = False

        try:
            self._addrs = rpyc.discover(self._service_name, host=self._ip, registrar=self._registrar)
            self._discovered_service = True
            self._remote_host_ip, self._port = self._addrs[0]
            try:
                self._connection = rpyc.connect(self._remote_host_ip, self._port)
                self._connected = True
            except socket.error:
                pass
        except DiscoveryError:
            pass

    def is_service_discovered(self):
        return self._discovered_service

    def is_connected(self):
        return self._connected

    @property
    def service_name(self):
        return self._service_name

    @property
    def database(self):
        return self._database

    @property
    def host_name(self):
        return self._host_name

    @property
    def ip(self):
        return self._ip

    @property
    def port(self):
        return self._port

    @property
    def root(self):
        if self._connection:
            return self._connection.root

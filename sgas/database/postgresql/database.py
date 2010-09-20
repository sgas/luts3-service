"""
Implementation of ISGASDatabase interface for PostgreSQL.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import decimal

import psycopg2

from zope.interface import implements

from twisted.python import log
from twisted.internet import defer
from twisted.enterprise import adbapi
from twisted.application import service

from sgas.database import ISGASDatabase, error
from sgas.database.postgresql import urparser, updater



DEFAULT_POSTGRESQL_PORT = 5432



class PostgreSQLDatabase(service.Service):

    implements(ISGASDatabase)

    def __init__(self, connect_info, checker):
        self.connect_info = connect_info
        self.dbpool = self._setupPool(self.connect_info)
        self.updater = updater.AggregationUpdater(self.dbpool)
        self.checker = checker


    def _setupPool(self, connect_info):
        args = [ e or None for e in connect_info.split(':') ]
        host, port, database, user, password, _ = args
        if port is None:
            port = DEFAULT_POSTGRESQL_PORT
        return adbapi.ConnectionPool('psycopg2', host=host, port=port, database=database, user=user, password=password)


    def startService(self):
        service.Service.startService(self)
        return self.updater.startService()


    def stopService(self):
        service.Service.stopService(self)
        return self.updater.stopService()


    @defer.inlineCallbacks
    def insert(self, usagerecord_data, insert_identity=None, insert_hostname=None, retry=False):
        # inserts usage record
        arg_list = urparser.buildArgList(usagerecord_data, insert_identity=insert_identity, insert_hostname=insert_hostname)

        self._checkIdentityConsistency(insert_identity, arg_list)

        try:
            id_dict = {}
            conn = adbapi.Connection(self.dbpool)
            try:
                trans = adbapi.Transaction(self, conn)

                for args in arg_list:
                    yield trans.callproc('urcreate', args)
                    r = yield trans.fetchall()
                    record_id, row_id = r[0][0]
                    id_dict[record_id] = str(row_id)

                trans.close()
                conn.commit()
                log.msg('Database: %i records inserted' % len(id_dict))
                # NOTIFY does not appear to work under adbapi, so we just do the notification here
                self.updater.updateNotification()
                defer.returnValue(id_dict)
            except:
                conn.rollback()
                raise

        except psycopg2.OperationalError, e:
            if 'Connection refused' in str(e):
                raise error.DatabaseUnavailableError(str(e))
            raise # re-raise current exception
        except psycopg2.InterfaceError, e:
            # this usually happens if the database was restarted,
            # and the existing connection to the database was closed
            if not retry:
                log.msg('Got interface error while attempting insert (%s), attempting to reconnect' % str(e))
                self.dbpool = self._setupPool(self.connect_info)
                yield self.insert(usagerecord_data, insert_identity=insert_identity,
                                  insert_hostname=insert_hostname, retry=True)
            if retry:
                log.msg('Got interface error after retrying to connect, bailing out.')
                raise error.DatabaseUnavailableError(str(e))

        except Exception, e:
            log.msg('Unexpected database error')
            log.err(e)
            raise


    @defer.inlineCallbacks
    def query(self, query, query_args=None, retry=False):

        def buildValue(value):
            if type(value) in (unicode, str, int, long, float, bool):
                return value
            if isinstance(value, decimal.Decimal):
                sv = str(value)
                return int(sv) if sv.isalnum() else float(sv)
            # bad catch-all
            return str(value)

        try:
            query_result = yield self.dbpool.runQuery(query, query_args)
            results = []
            for row in query_result:
                results.append( [ buildValue(e) for e in row ] )
            defer.returnValue(results)
        except psycopg2.InterfaceError, e:
            # this usually happens if the database was restarted,
            # and the existing connection to the database was closed
            if not retry:
                log.msg('Got interface error while querying database(%s), attempting to reconnect' % str(e))
                self.dbpool = self._setupPool(self.connect_info)
                yield self.query(query, query_args, retry=True)
            if retry:
                log.msg('Got interface error after retrying to connect, bailing out.')
                raise error.DatabaseUnavailableError(str(e))


    def _checkIdentityConsistency(self, insert_identity, arg_list):
        # check the consistency between machine_name in records and the identity of the inserter

        docs = [ dict(zip(urparser.ARG_LIST, args)) for args in arg_list ]

        for doc in docs:
            machine_name = doc.get('machine_name')
            if not self.checker.isInsertAllowed(insert_identity, machine_name):
                MSG = 'Machine name (%s) does not match FQDN of identity (%s) to sufficient degree'
                raise error.SecurityError(MSG % (machine_name, insert_identity))


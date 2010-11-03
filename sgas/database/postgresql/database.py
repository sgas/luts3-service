"""
Implementation of ISGASDatabase interface for PostgreSQL.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import decimal

import psycopg2
import psycopg2.extensions # not used, but enables tuple adaption

from zope.interface import implements

from twisted.python import log
from twisted.internet import defer
from twisted.enterprise import adbapi
from twisted.application import service

from sgas.database import ISGASDatabase, error
from sgas.database.postgresql import urconverter, updater



DEFAULT_POSTGRESQL_PORT = 5432



class _DatabasePoolProxy:
    # abstraction over a database pool object, so we can provide a sensible way
    # to replcate the pool if something goes wrong.

    def __init__(self, connect_info):

        self.connect_info = connect_info
        self.dbpool = None
        self.reconnect()


    def _setupPool(self, connect_info):
        args = [ e or None for e in connect_info.split(':') ]
        host, port, database, user, password, _ = args
        if port is None:
            port = DEFAULT_POSTGRESQL_PORT
        return adbapi.ConnectionPool('psycopg2', host=host, port=port, database=database, user=user, password=password)


    def reconnect(self):
        # close dbpool before creating an new (to stop the threadpool mainly)
        if self.dbpool is not None:
            log.msg('Closing failed connection before reconnecting.')
            log.msg('This will likely cause psycopg2.InterfaceError, as the connection is half-closed.')
            self.dbpool.close()

        self.dbpool = self._setupPool(self.connect_info)




class PostgreSQLDatabase(service.Service):

    implements(ISGASDatabase)

    def __init__(self, connect_info):
        self.pool_proxy = _DatabasePoolProxy(connect_info)
        #self.dbpool = self._setupPool(self.connect_info)
        self.updater = updater.AggregationUpdater(self.pool_proxy)


    def startService(self):
        service.Service.startService(self)
        return self.updater.startService()


    def stopService(self):
        service.Service.stopService(self)
        return self.updater.stopService()


    @defer.inlineCallbacks
    def insert(self, usagerecord_docs, retry=False):
        # inserts usage record

        arg_list = urconverter.createInsertArguments(usagerecord_docs)

        try:
            id_dict = {}
            conn = adbapi.Connection(self.pool_proxy.dbpool)
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
            except psycopg2.InterfaceError:
                # error (usually) implies that the connection is closed, can't rollback
                raise
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
                log.msg('Got interface error while attempting insert: %s.' % str(e))
                log.msg('Attempting to reconnect.')
                self.pool_proxy.reconnect()
                yield self.insert(usagerecord_docs, retry=True)
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
            query_result = yield self.pool_proxy.dbpool.runQuery(query, query_args)
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


"""
High-level inteface for PostgreSQL.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import types
import decimal

import psycopg2
import psycopg2.extensions # not used, but enables tuple adaption
import psycopg2.extras

from twisted.python import log
from twisted.internet import defer
from twisted.enterprise import adbapi
from twisted.application import service

from sgas.database import error
#from sgas.database.postgresql import updater


DEFAULT_POSTGRESQL_PORT = 5432

SQL_SERIALIZABLE_TRANSACTION = '''SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'''


class _DatabasePoolProxy:
    # abstraction over a database pool object, so we can provide a sensible way
    # to replace the pool if something goes wrong.

    def __init__(self, connect_info):

        self.connect_info = connect_info
        self.dbpool = None
        self.reconnect()


    def _setupPool(self, connect_info):
        args = [ e or None for e in connect_info.split(':') ]
        host, port, database, user, password = args[:5]
        if port is None:
            port = DEFAULT_POSTGRESQL_PORT
        return adbapi.ConnectionPool('psycopg2', host=host, port=port, database=database, user=user, password=password)


    def reconnect(self):
        # close dbpool before creating an new (to stop the threadpool mainly)
        if self.dbpool is not None:
            log.msg('Closing failed connection before reconnecting.', system='sgas.PostgreSQLDatabase')
            log.msg('This will likely cause psycopg2.InterfaceError, as the connection is half-closed.', system='sgas.PostgreSQLDatabase')
            self.dbpool.close()

        self.dbpool = self._setupPool(self.connect_info)




class PostgreSQLDatabase(service.MultiService):

    service = []
    
    def __init__(self, connect_info):
        service.MultiService.__init__(self)
        self.pool_proxy = _DatabasePoolProxy(connect_info)


    def startService(self):
        service.MultiService.startService(self)
        return defer.DeferredList(map(lambda s: s.startService(),self.service))        


    def stopService(self):
        service.MultiService.stopService(self)
        return defer.DeferredList(map(lambda s: s.stopService(),self.service))


    def attachService(self,service):
        self.service += [service]


    @defer.inlineCallbacks
    def query(self, query, query_args=None, retry=False):

        def buildValue(value):
            if type(value) in (unicode, str, int, long, float, bool, types.NoneType):
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
        except (psycopg2.InterfaceError, psycopg2.OperationalError), e:
            # this usually happens if the database was restarted,
            # and the existing connection to the database was closed
            if not retry:
                log.msg('Got interface error while querying database(%s), attempting to reconnect' % str(e), system='sgas.PostgreSQLDatabase')
                self.pool_proxy.reconnect()
                yield self.query(query, query_args, retry=True)
            if retry:
                log.msg('Got interface error after retrying to connect, bailing out.', system='sgas.PostgreSQLDatabase')
                raise error.DatabaseUnavailableError(str(e))

    @defer.inlineCallbacks
    def recordInserter(self, type, proc, arg_list, retry=False):
        try:
            id_dict = {}
            conn = adbapi.Connection(self.pool_proxy.dbpool)
            try:
                trans = adbapi.Transaction(self, conn)

                for args in arg_list:
                    yield trans.callproc(proc, args)
                    r = yield trans.fetchall()
                    record_id, row_id = r[0][0]
                    id_dict[record_id] = str(row_id)

                trans.close()
                conn.commit()
                log.msg('Database: %i %s records inserted' % (len(id_dict), type), system='sgas.PostgreSQLDatabase')
                defer.returnValue(id_dict)
            except psycopg2.InterfaceError:
                # this usually implies that the connection is closed (can't rollback)
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
                log.msg('Got interface error while attempting insert: %s.' % str(e), system='sgas.PostgreSQLDatabase')
                log.msg('Attempting to reconnect.', system='sgas.PostgreSQLDatabase')
                self.pool_proxy.reconnect()
                self.recordInserter(type, proc, arg_list, retry=True)
            if retry:
                log.msg('Got interface error after retrying to connect, bailing out.', system='sgas.PostgreSQLDatabase')
                raise error.DatabaseUnavailableError(str(e))

        except Exception, e:
            log.msg('Unexpected database error', system='sgas.PostgreSQLDatabase')
            log.err(e, system='sgas.PostgreSQLDatabase')
            raise

    @defer.inlineCallbacks
    def dictquery(self, query, query_args=None, retry=False):

        def buildValue(value):
            if type(value) in (unicode, str, int, long, float, bool, types.NoneType):
                return value
            if isinstance(value, decimal.Decimal):
                sv = str(value)
                return int(sv) if sv.isalnum() else float(sv)
            # bad catch-all
            return str(value)

        def conn(conn, query, query_args):
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query,query_args)
            return cur.fetchall()

        try:
            query_result = yield self.pool_proxy.dbpool.runWithConnection(conn, query, query_args)
            results = []
            for row in query_result:
                results.append(dict([(k,buildValue(row[k])) for k in row]))
            defer.returnValue(results)
        except (psycopg2.InterfaceError, psycopg2.OperationalError), e:
            # this usually happens if the database was restarted,
            # and the existing connection to the database was closed
            if not retry:
                log.msg('Got interface error while querying database(%s), attempting to reconnect' % str(e), system='sgas.PostgreSQLDatabase')
                self.pool_proxy.reconnect()
                yield self.query(query, query_args, retry=True)
            if retry:
                log.msg('Got interface error after retrying to connect, bailing out.', system='sgas.PostgreSQLDatabase')
                raise error.DatabaseUnavailableError(str(e))


    @defer.inlineCallbacks
    def updateAggregator(self,aggregator,service=None,retry=False):
        try:
            conn = adbapi.Connection(self.pool_proxy.dbpool)
            while True:
                try:
                    txn = adbapi.Transaction(self, conn)

                    # the update_uraggregate function requires serializable isolation level
                    # in order to execute correctly
                    txn.execute(SQL_SERIALIZABLE_TRANSACTION)
                    yield txn.callproc(aggregator)
                    idmn = txn.fetchall()
                    txn.close()
                    conn.commit()

                    if idmn in (None, [(None,)]): # empty array -> response when all done
                        break
                    else:
                        insert_date, machine_name = idmn[0][0]
                        log.msg('Aggregation(%s) updated: %s / %s' % (aggregator,insert_date, machine_name), system='sgas.AggregationUpdater')
                    if service and service.stopping:
                        break
                except psycopg2.InterfaceError, e:
                    # typically means we lost the connection due to a db restart
                    if retry:
                        log.msg('Reconnect in update failed, bailing out.', system='sgas.AggregationUpdater')
                        raise
                    else:
                        log.msg('Got InterfaceError while attempting update: %s.' % str(e), system='sgas.AggregationUpdater')
                        log.msg('Attempting reconnect.', system='sgas.AggregationUpdater')
                        retry = True
                        self.pool_proxy.reconnect()
                except:
                    conn.rollback()
                    raise

        except Exception, e:
            log.err(e, system='sgas.AggregationUpdater')
            raise

        finally:
            conn.close()

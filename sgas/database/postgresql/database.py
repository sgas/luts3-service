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

from sgas.database import ISGASDatabase, error, queryparser
from sgas.database.postgresql import urparser, queryengine



DEFAULT_POSTGRESQL_PORT = 5432



class PostgreSQLDatabase:

    implements(ISGASDatabase)

    def __init__(self, connect_info):

        args = [ e or None for e in connect_info.split(':') ]
        host, port, database, user, password, _ = args
        if port is None:
            port = DEFAULT_POSTGRESQL_PORT
        self.dbpool = adbapi.ConnectionPool('psycopg2', host=host, port=port, database=database, user=user, password=password)


    @defer.inlineCallbacks
    def insert(self, usagerecord_data, insert_identity=None, insert_hostname=None):
        # inserts usage record
        arg_list = urparser.buildArgList(usagerecord_data, insert_identity=insert_identity, insert_hostname=insert_hostname)
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
                defer.returnValue(id_dict)
            except:
                conn.rollback()
                raise

        except psycopg2.OperationalError, e:
            if 'Connection refused' in str(e):
                raise error.DatabaseUnavailableError(str(e))
            raise # re-raise current exception
        except Exception, e:
            log.msg('Unexpected database error')
            log.err(e)
            raise


    @defer.inlineCallbacks
    def query(self, selects, filters=None, groups=None, orders=None):

        def buildValue(value):
            if type(value) in (unicode, str, int, float, bool):
                return value
            if isinstance(value, decimal.Decimal):
                sv = str(value)
                return int(sv) if sv.isalnum() else float(sv)
            # bad catch-all
            return str(value)

        q = queryparser.QueryParser(selects, filters, groups, orders)

        query_stm = queryengine.buildQuery(q)

        query_result = yield self.dbpool.runQuery(query_stm)

        results = []
        for row in query_result:
            results.append( [ buildValue(e) for e in row ] )

        defer.returnValue(results)


"""
Implementation of ISGASDatabase interface for PostgreSQL.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from pyPgSQL import libpq, PgSQL

from zope.interface import implements

from twisted.internet import defer

from sgas.database import ISGASDatabase, error, queryparser
from sgas.database.postgresql import urparser, queryengine

from twisted.enterprise import adbapi 



class PostgreSQLDatabase:

    implements(ISGASDatabase)

    def __init__(self, connect_info):

        self.dbpool = adbapi.ConnectionPool('pyPgSQL.PgSQL', connect_info)


    @defer.inlineCallbacks
    def insert(self, usagerecord_data):
        # inserts usage record
        try:
            insert_stms = urparser.usageRecordsToInsertStatements(usagerecord_data)
            result = yield self.dbpool.runQuery(insert_stms)
            db_ids = [ r.get('urcreate', None) for r in result ]
            defer.returnValue(db_ids)
        except libpq.DatabaseError, e:
            if 'Connection refused' in e.message:
                raise error.DatabaseUnavailableError(e.message)
            raise # re-raise current exception


    @defer.inlineCallbacks
    def query(self, selects, filters=None, groups=None, orders=None):

        def buildValue(value):
            if type(value) in (unicode, str, int, float, bool):
                return value
            if isinstance(value, PgSQL.PgNumeric):
                if value.getPrecision() == 0:
                    return int(value)
                else:
                    return float(value)
            # bad catch-all
            return str(value)

        q = queryparser.QueryParser(selects, filters, groups, orders)

        query_stm = queryengine.buildQuery(q)

        query_result = yield self.dbpool.runQuery(query_stm)

        results = []
        for row in query_result:
            results.append( [ buildValue(e) for e in row ] )

        defer.returnValue(results)


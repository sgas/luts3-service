#
# Database unit tests
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009, 2010)

import os
import time

from twisted.trial import unittest
from twisted.internet import defer

from . import ursampledata



SGAS_TEST_FILE = os.path.join(os.path.expanduser('~'), '.sgas-test')



class GenericDatabaseTest:

    def fetchUsageRecord(self, record_id):
        # fetch an individual usage record from the underlying database given a record id
        # should return none if the record does not exist
        raise NotImplementedError('fetching individual usage record is not implemented in generic test (nor should it be)')


    @defer.inlineCallbacks
    def testSingleInsert(self):

        doc_ids = yield self.db.insert(ursampledata.UR1)
        self.failUnlessEqual(len(doc_ids), 1)

        doc = yield self.fetchUsageRecord(ursampledata.UR1_ID)

        self.failUnlessEqual(doc.get('record_id', None), ursampledata.UR1_ID)
        self.failUnlessEqual(doc.get('job_name', None),  'test job 1')


    @defer.inlineCallbacks
    def testExistenceBeforeAndAfterInsert(self):

        doc = yield self.fetchUsageRecord(ursampledata.UR2_ID)
        self.failUnlessEqual(doc, None)

        doc_ids = yield self.db.insert(ursampledata.UR2)
        self.failUnlessEqual(len(doc_ids), 1)

        doc = yield self.fetchUsageRecord(ursampledata.UR2_ID)

        self.failUnlessEqual(doc.get('record_id', None), ursampledata.UR2_ID, doc)
        self.failUnlessEqual(doc.get('job_name', None),  'test job 2')


    @defer.inlineCallbacks
    def testCompoundInsert(self):

        doc_ids = yield self.db.insert(ursampledata.CUR)

        self.failUnlessEqual(len(doc_ids), 2)

        for ur_id in ursampledata.CUR_IDS:
            doc = yield self.fetchUsageRecord(ur_id)
            self.failUnlessEqual(doc.get('record_id', None), ur_id)


class QueryDatabaseTest:

    def triggerAggregateUpdate(self):
        # trigger an update of the aggregated information in the underlying database (if needed)
        raise NotImplementedError('updating aggregated information is not implemented in generic test (nor should it be)')


    @defer.inlineCallbacks
    def testBasicQuery(self):

        yield self.db.insert(ursampledata.UR1)
        yield self.db.insert(ursampledata.UR2)

        yield self.triggerAggregateUpdate()

        result = yield self.db.query('distinct:user_identity')
        self.failUnlessEqual(result, [['/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User']])


    @defer.inlineCallbacks
    def testGroupQuery(self):

        yield self.db.insert(ursampledata.UR1)
        yield self.db.insert(ursampledata.UR2)
        yield self.triggerAggregateUpdate()

        result = yield self.db.query('user_identity, sum:n_jobs', groups='user_identity')
        self.failUnlessEqual(result, [['/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User', 2]])


    @defer.inlineCallbacks
    def testFilterQuery(self):

        yield self.db.insert(ursampledata.CUR)
        yield self.triggerAggregateUpdate()

        result = yield self.db.query('user_identity, sum:n_jobs', filters='machine_name = fyrgrid.grid.aau.dk', groups='user_identity')
        self.failUnlessEqual(result, [['/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User', 1]])


    @defer.inlineCallbacks
    def testOrderQuery(self):

        yield self.db.insert(ursampledata.CUR)
        yield self.triggerAggregateUpdate()

        result = yield self.db.query('machine_name', orders='machine_name')
        self.failUnlessEqual(result, [['benedict.grid.aau.dk'], ['fyrgrid.grid.aau.dk']])



class CouchDBTest(GenericDatabaseTest, unittest.TestCase):

    couch_dbms = None
    couch_database = None
    couch_database_name = None


    @defer.inlineCallbacks
    def fetchUsageRecord(self, record_id):

        import json
        from sgas.database.couchdb import urparser, couchdbclient

        db_id = urparser.createID(record_id)
        try:
            ur_doc = yield self.couch_database.retrieveDocument(db_id)
            defer.returnValue(ur_doc)
        except couchdbclient.NoSuchDocumentError:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def triggerAggregateUpdate(self):
        yield defer.succeed(None)
        defer.returnValue(None)


    @defer.inlineCallbacks
    def setUp(self):

        import json
        from sgas.database.couchdb import database, couchdbclient

        config = json.load(file(SGAS_TEST_FILE))
        url = str(config['couchdb.url'])

        base_url, self.couch_database_name = url.rsplit('/', 1)
        self.couch_dbms = couchdbclient.CouchDB(base_url)
        self.couch_database = yield self.couch_dbms.createDatabase(self.couch_database_name)

        self.db = database.CouchDBDatabase(url, 0)
        yield self.db.startService()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.stopService()
        yield self.couch_dbms.deleteDatabase(self.couch_database_name)



class PostgreSQLTestCase(GenericDatabaseTest, QueryDatabaseTest, unittest.TestCase):

    @defer.inlineCallbacks
    def fetchUsageRecord(self, record_id):

        import sgas.database.postgresql.urparser as pgurparser

        stm = "SELECT * from usagerecords where record_id = %s"
        res = yield self.postgres_dbpool.runQuery(stm, (record_id,))

        if res == []:
            defer.returnValue(None)
        elif len(res) == 1:
            ur_doc = dict( zip(pgurparser.ARG_LIST, res[0]) )
            defer.returnValue(ur_doc)
        else:
            self.fail('Multiple results returned for a single usage record')


    @defer.inlineCallbacks
    def triggerAggregateUpdate(self):
        # should update the uraggregate table here
        yield self.db.updater.performUpdate()


    def setUp(self):

        import json
        from twisted.enterprise import adbapi
        from sgas.database.postgresql import database

        config = json.load(file(SGAS_TEST_FILE))
        db_url = config['postgresql.url']

        args = [ e or None for e in db_url.split(':') ]
        host, port, db, user, password, _ = args
        if port is None: port = 5432

        self.postgres_dbpool = adbapi.ConnectionPool('psycopg2', host=host, port=port, database=db, user=user, password=password)

        self.db = database.PostgreSQLDatabase(db_url, 0)
        return self.db.startService()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.stopService()
        # delete all ur rows in the database
        delete_stms = \
        "TRUNCATE uraggregated;"            + \
        "TRUNCATE uraggregated_update;"     + \
        "TRUNCATE usagedata;"               + \
        "TRUNCATE globalusername CASCADE;"  + \
        "TRUNCATE insertidentity CASCADE;"  + \
        "TRUNCATE machinename    CASCADE;"  + \
        "TRUNCATE voinformation  CASCADE;"
        yield self.postgres_dbpool.runOperation(delete_stms)
        yield self.postgres_dbpool.close()


    @defer.inlineCallbacks
    def testUpdateAfterSingleInsert(self):

        doc_ids = yield self.db.insert(ursampledata.UR1)

        rows = yield self.postgres_dbpool.runQuery('SELECT * from uraggregated_update')

        self.failUnlessEqual(len(rows), 1)

        # do the date dance!
        gmt = time.gmtime()
        date = '%s-%s-%s' % (gmt.tm_mday, gmt.tm_mon, gmt.tm_year)
        mxd = rows[0][0]
        row_date = '%s-%s-%s' % (mxd.day, mxd.month, mxd.year)

        self.failUnlessEqual( [ (row_date, rows[0][1]) ], [ (date, 'benedict.grid.aau.dk') ])


    @defer.inlineCallbacks
    def testUpdateAfterTrigger(self):

        doc_ids = yield self.db.insert(ursampledata.CUR)
        yield self.triggerAggregateUpdate()

        rows = yield self.postgres_dbpool.runQuery('SELECT * from uraggregated_update')
        self.failUnlessEqual(len(rows), 0)



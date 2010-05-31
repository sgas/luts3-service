#
# Database unit tests
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009, 2010)

import os

from twisted.trial import unittest
from twisted.internet import defer

from sgas.server import database

from . import ursampledata



SGAS_TEST_FILE = os.path.join(os.path.expanduser('~'), '.sgas-test')



class GenericDatabaseTest:

    def fetchUsageRecord(self, record_id):
        # fetch an individual usage record from the underlying database given a record id
        # should return none if the record does not exist
        raise NotImplementedError('fetching individual usage record not support in generic test (nor should it be)')


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
    def setUp(self):

        import json
        from sgas.database.couchdb import database, couchdbclient

        config = json.load(file(SGAS_TEST_FILE))
        url = str(config['couchdb.url'])

        base_url, self.couch_database_name = url.rsplit('/', 1)
        self.couch_dbms = couchdbclient.CouchDB(base_url)
        self.couch_database = yield self.couch_dbms.createDatabase(self.couch_database_name)

        self.db = database.CouchDBDatabase(url)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.couch_dbms.deleteDatabase(self.couch_database_name)



class PostgreSQLTestCase(GenericDatabaseTest, unittest.TestCase):

    @defer.inlineCallbacks
    def fetchUsageRecord(self, record_id):

        stm = "SELECT * from usagerecords where record_id = %s"
        res = yield self.postgres_dbpool.runQuery(stm, record_id)

        if res == []:
            defer.returnValue(None)
        elif len(res) == 1:
            ur_doc = dict(res[0].items())
            defer.returnValue(ur_doc)
        else:
            self.fail('Multiple results returned for a single usage record')


    def setUp(self):

        import json
        from twisted.enterprise import adbapi
        from sgas.database.postgresql import database

        config = json.load(file(SGAS_TEST_FILE))
        db_url = config['postgresql.url']

        self.postgres_dbpool = adbapi.ConnectionPool('pyPgSQL.PgSQL', db_url)

        self.db = database.PostgreSQLDatabase(db_url)


    @defer.inlineCallbacks
    def tearDown(self):
        # delete all ur rows in the database
        delete_stms = \
        "TRUNCATE usagedata;"               + \
        "TRUNCATE globalusername CASCADE;"  + \
        "TRUNCATE insertidentity CASCADE;"  + \
        "TRUNCATE machinename    CASCADE;"  + \
        "TRUNCATE voinformation  CASCADE;"
        yield self.postgres_dbpool.runOperation(delete_stms)
        yield self.postgres_dbpool.close()



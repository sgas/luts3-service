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

        doc = yield self.fetchUsageRecord(ursampledata.UR1_ID)

        self.failUnlessIn(ursampledata.UR1_ID, doc)
        self.failUnlessIn("test job 1", doc)


    @defer.inlineCallbacks
    def testExistenceBeforeAndAfterInsert(self):

        doc = yield self.fetchUsageRecord(ursampledata.UR2_ID)
        self.failUnlessEqual(doc, None)

        doc_ids = yield self.db.insert(ursampledata.UR2)

        doc = yield self.fetchUsageRecord(ursampledata.UR2_ID)

        self.failUnlessIn(ursampledata.UR2_ID, doc)
        self.failUnlessIn("test job 2", doc)


    @defer.inlineCallbacks
    def testCompoundInsert(self):

        doc_ids = yield self.db.insert(ursampledata.CUR)

        self.failUnlessEqual(len(doc_ids), 2)

        for ur_id in ursampledata.CUR_IDS:
            doc = yield self.fetchUsageRecord(ur_id)
            self.failUnlessIn(ur_id, doc)



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
            defer.returnValue(json.dumps(ur_doc))
        except couchdbclient.NoSuchDocumentError:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def setUp(self):

        import json
        from sgas.database.couchdb import database, couchdbclient

        config = json.load(file(SGAS_TEST_FILE))
        url = str(config['db.url'])

        base_url, self.couch_database_name = url.rsplit('/', 1)
        self.couch_dbms = couchdbclient.CouchDB(base_url)
        self.couch_database = yield self.couch_dbms.createDatabase(self.couch_database_name)

        self.db = database.CouchDBDatabase(url)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.couch_dbms.deleteDatabase(self.couch_database_name)




#class PostgreSQLTestCase(GenericDatabaseTest, unittest.TestCase):
#
#    @defer.inlineCallbacks
#    def setUp(self):
#        config = json.load(file(SGAS_TEST_FILE))
#




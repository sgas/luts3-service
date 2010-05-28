#
# Database unit tests
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import os
import json
import urllib

from twisted.trial import unittest
from twisted.internet import defer

from sgas.common import couchdb
from sgas.database.couchdb import urparser
from sgas.server import database #, usagerecord

from . import ursampledata



SGAS_TEST_FILE = os.path.join(os.path.expanduser('~'), '.sgas-test')



class GenericDatabaseTest:

    @defer.inlineCallbacks
    def testSingleInsert(self):

        ur1_id = 'gsiftp://example.org/jobs/1'
        ur1_hs = urparser.createID(ur1_id)

        doc_ids = yield self.ur_db.insertUsageRecords(ur.UR1)
        self.failUnlessEqual(doc_ids, {ur1_id: {'id':ur1_hs}})

        doc = yield self.ur_db.getUsageRecord(ur1_hs)
        self.failUnlessIn(ur1_id, doc)
        self.failUnlessIn("test job 1", doc)

    testSingleInsert.skip = 'New database test not ready'

    @defer.inlineCallbacks
    def testCompoundInsert(self):

        cur_id1 = 'gsiftp://example.org/jobs/3'
        cur_id2 = 'gsiftp://example.org/jobs/4'
        cur_hs1 = urparser.createID(cur_id1)
        cur_hs2 = urparser.createID(cur_id2)

        doc_ids = yield self.ur_db.insertUsageRecords(ur.CUR)
        self.failUnlessEqual(len(doc_ids), 2)
        wanted_result = {cur_id1: {'id':cur_hs1}, cur_id2: {'id':cur_hs2}}
        self.failUnlessEqual(doc_ids, wanted_result)

        doc1 = yield self.ur_db.getUsageRecord(cur_hs1)
        self.failUnlessIn("test job 3", doc1)
        doc2 = yield self.ur_db.getUsageRecord(cur_hs2)
        self.failUnlessIn("test job 4", doc2)

    testCompoundInsert.skip = 'New database test not ready'




class CouchDBTest(GenericDatabaseTest, unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        config = json.load(file(SGAS_TEST_FILE))
        url = str(config['db.url'])

        base_url, self.db_name = url.rsplit('/', 1)
        self.cdc = couchdb.CouchDB(base_url)
        self.cdb = yield self.cdc.createDatabase(self.db_name)
        self.ur_db = database.UsageRecordDatabase(self.cdb)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.cdc.deleteDatabase(self.db_name)


class PostgreSQLTestCase(GenericDatabaseTest, unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        config = json.load(file(SGAS_TEST_FILE))





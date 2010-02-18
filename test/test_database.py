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

from sgas.server import couchdb, database, usagerecord



SGAS_TEST_FILE = os.path.join(os.path.expanduser('~'), '.sgas-test')


UR1 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/1" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/1</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 1</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-07T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-07T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT1S</ur:WallDuration>
</ur:JobUsageRecord>
"""

UR2 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/2" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/2</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 2</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-08T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-08T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT2S</ur:WallDuration>
</ur:JobUsageRecord>
"""

CUR = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:UsageRecords xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
  <ur:JobUsageRecord>
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/3" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/3</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 3</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-08T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-08T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT3S</ur:WallDuration>
  </ur:JobUsageRecord>
  <ur:JobUsageRecord>
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/4" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/4</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 4</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-08T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-08T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT4S</ur:WallDuration>
  </ur:JobUsageRecord>
</ur:UsageRecords>
"""


class DatabaseTest(unittest.TestCase):


    @defer.inlineCallbacks
    def setUp(self):
        config = json.load(file(SGAS_TEST_FILE))
        # convert unicode to regular strings, as the sedna module requires this
        url = str(config['db.url'])
        base_url, self.db_name = url.rsplit('/', 1)
        self.cdc = couchdb.CouchDB(base_url)
        self.cdb = yield self.cdc.createDatabase(self.db_name)
        self.ur_db = database.UsageRecordDatabase(self.cdb)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.cdc.deleteDatabase(self.db_name)


    @defer.inlineCallbacks
    def testSingleInsert(self):

        ur1_id = 'gsiftp://example.org/jobs/1'
        ur1_hs = usagerecord.createID(ur1_id)

        doc_ids = yield self.ur_db.insertUsageRecords(UR1)
        self.failUnlessEqual(doc_ids, {ur1_id: {'id':ur1_hs}})

        doc = yield self.ur_db.getUsageRecord(ur1_hs)
        self.failUnlessIn(ur1_id, doc)
        self.failUnlessIn("test job 1", doc)


    @defer.inlineCallbacks
    def testCompoundInsert(self):

        cur_id1 = 'gsiftp://example.org/jobs/3'
        cur_id2 = 'gsiftp://example.org/jobs/4'
        cur_hs1 = usagerecord.createID(cur_id1)
        cur_hs2 = usagerecord.createID(cur_id2)

        doc_ids = yield self.ur_db.insertUsageRecords(CUR)
        self.failUnlessEqual(len(doc_ids), 2)
        wanted_result = {cur_id1: {'id':cur_hs1}, cur_id2: {'id':cur_hs2}}
        self.failUnlessEqual(doc_ids, wanted_result)

        doc1 = yield self.ur_db.getUsageRecord(cur_hs1)
        self.failUnlessIn("test job 3", doc1)
        doc2 = yield self.ur_db.getUsageRecord(cur_hs2)
        self.failUnlessIn("test job 4", doc2)



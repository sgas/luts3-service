#
# Database unit tests
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import os
import json
import urllib

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.web import error as weberror

from sgas.server import couchdb, database, usagerecord, view, setup

from . import rclient, ur


SGAS_TEST_FILE = os.path.join(os.path.expanduser('~'), '.sgas-test')



class FakeAuthorizer:

    def isAllowed(self, subject, action, context=None):
        return True



class ResourceTest(unittest.TestCase):

    port = 6180

    @defer.inlineCallbacks
    def setUp(self):
        config = json.load(file(SGAS_TEST_FILE))
        # convert unicode to regular strings, as the sedna module requires this
        url = str(config['db.url'])
        base_url, self.db_name = url.rsplit('/', 1)

        self.cdc = couchdb.CouchDB(base_url)
        self.cdb = yield self.cdc.createDatabase(self.db_name)
        self.ur_db = yield database.UsageRecordDatabase(self.cdb)

        site = setup.createSite(self.ur_db, FakeAuthorizer(), '/tmp') # don't use views

        self.iport = reactor.listenTCP(self.port, site)

        self.service_url = 'http://localhost:%i/sgas' % self.port


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.iport.stopListening()
        yield self.cdc.deleteDatabase(self.db_name)


    @defer.inlineCallbacks
    def testInsertRetrieval(self):
        insert_url = self.service_url + '/ur'

        d, f = rclient.httpRequest(insert_url, method='POST', payload=ur.UR1)
        r = yield d
        self.failUnlessEqual(f.status, '200')

        d = json.loads(r)
        record_id = str(d.values()[0]['id'])
        ur_url = insert_url + '/recordid/' + record_id

        d, f = rclient.httpRequest(ur_url, method='GET')
        r = yield d
        self.failUnlessEqual(f.status, '200')
        d = json.loads(r)
        self.failUnlessEqual(record_id, d['_id'])


    @defer.inlineCallbacks
    def testUnavailableDatabase(self):

        # first stop the "real" service
        yield self.iport.stopListening()

        # setup a new service with the "bad" database

        cdb = couchdb.Database('http://localhost:9999/nosuchdb')
        views = { 'testview' : view.ViewDefinition('testview', 'fakedesign', 'fakeview') }
        ur_db = yield database.UsageRecordDatabase(cdb, views)

        site = setup.createSite(ur_db, FakeAuthorizer(), '/tmp')
        self.iport = reactor.listenTCP(self.port, site)

        # -- start the actual test

        insert_url = self.service_url + '/ur'
        d, f = rclient.httpRequest(insert_url, method='POST', payload=ur.UR1)
        try:
            r = yield d
            self.fail('Request should have failed with 503')
        except weberror.Error, e:
            self.failUnlessEqual(e.status, '503')

        ur_url = self.service_url + '/ur/recordid/abcdef123456'
        d, f = rclient.httpRequest(ur_url, method='GET')
        try:
            r = yield d
            self.fail('Request should have failed with 503')
        except weberror.Error, e:
            self.failUnlessEqual(e.status, '503')

# for some reason this fails
#        view_url = self.service_url + '/view/testview'
#        d, f = rclient.httpRequest(view_url, method='GET')
#        try:
#            r = yield d
#            self.fail('Request should have failed with 503')
#        except weberror.Error, e:
#            self.failUnlessEqual(e.status, '503')


import os
import json

from twisted.internet import defer
from twisted.trial import unittest

from sgas.server import couchdb



SGAS_TEST_FILE = os.path.join(os.path.expanduser('~'), '.sgas-test')

DOC0 = {'k':0, '_id':'zero'}
DOC1 = {'k':1, 'thing':'yummie'}
DOC2 = {'k':2, '_id':'yummie'}



class CouchDBTest(unittest.TestCase):

    def setUp(self):
        config = json.load(file(SGAS_TEST_FILE))
        self.url = str(config['db.url'])
        self.base_url, self.db_name = self.url.rsplit('/', 1)


    @defer.inlineCallbacks
    def testEverything(self):

        #couch = couchdb.CouchDB('http://localhost:5984/')
        couch = couchdb.CouchDB(self.base_url)

        try:
            db = yield couch.createDatabase(self.db_name)

            res = yield db.createDocument(DOC0)
            doc_id = str(res['id'])
            self.failUnlessEqual(doc_id, 'zero')

            info = yield db.info()
            self.failUnlessEqual(info['doc_count'], 1)

            doc_ids = yield db.listDocuments()
            self.failUnlessEqual(doc_ids['rows'][0]['id'], 'zero')

            doc = yield db.retrieveDocument(doc_id)
            self.failUnlessEqual(doc['k'], 0)

            res = yield db.insertDocuments([DOC1, DOC2])

            info = yield db.info()
            self.failUnlessEqual(info['doc_count'], 3)

            #docs = yield db.retrieveDocuments(['yummie', 'zero'])

            yield db.deleteDocument(doc)
            info = yield db.info()
            self.failUnlessEqual(info['doc_count'], 2)

        finally:

            yield couch.deleteDatabase(self.db_name)


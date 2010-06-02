"""
Implementation of the SGAS database interface with CouchDB as backend.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Faciliy (2009, 2010)
"""


from zope.interface import implements

from twisted.python import log
from twisted.internet import defer

from sgas.database import ISGASDatabase
from sgas.database.couchdb import couchdbclient, urparser



class CouchDBDatabase:

    implements(ISGASDatabase)

    def __init__(self, couchdb_url):

        self.db = couchdbclient.Database(couchdb_url)


    @defer.inlineCallbacks
    def insert(self, usagerecord_data, insert_identity=None, insert_hostname=None):

        ur_docs = urparser.usageRecordsToCouchDBDocuments(usagerecord_data,
                                                          insert_identity=insert_identity,
                                                          insert_hostname=insert_hostname)

        # create dict with _id -> doc mapping for database insertion
        docs = dict( [ (ur_doc['_id'], ur_doc) for ur_doc in ur_docs ] )

        # insert documents
        result = yield self.db.insertDocuments(docs.values())
        log.msg('Inserted %i documents into database' % len(docs), system='sgas.database.couchdb')

        # create return data indicicating inserts and conflicts
        # create dict with record_id -> _id mapping
        idmap = dict( [ (ur_doc['record_id'], ur_doc['_id']) for ur_doc in ur_docs ] )
        # id -> result_doc mapping
        id_result = dict( [ (r['id'], r) for r in result ] )
        # record_id -> document _id (from database)
        return_data = dict( [ (record_id, id_result[id_]) for record_id, id_ in idmap.items() ] )

        defer.returnValue(return_data)



    def query(self, selects, filters=None, groups=None, orders=None):
        raise NotImplementedError('Query engine for couchdb is not implemented')


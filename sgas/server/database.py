"""
High level database access which incorportes usage records.
"""
import json

from twisted.python import log
from twisted.internet import defer

from sgas.server import usagerecord, convert



class InvalidViewError(Exception):
    """
    Raised when the specified view does not exist
    """
    pass



class UsageRecordDatabase:

    def __init__(self, db, views=None):
        self.db = db
        self.views = views or {}


    @defer.inlineCallbacks
    def insertUsageRecords(self, ur_data, identity=None, hostname=None):

        parser = usagerecord.UsageRecordParser()
        edocs = parser.getURDocuments(ur_data)

        docs = {}
        idmap = {}
        for edoc in edocs.values():
            doc = usagerecord.xmlToDict(edoc, insert_identity=identity, insert_hostname=hostname)
            docs[doc['_id']] = doc
            idmap[doc['record_id']] = doc['_id']

        result = yield self.db.insertDocuments(docs.values())
        log.msg('Inserted %i documents into database' % len(docs), system='sgas.UsageRecordDatabase')

        # create return data indicicating inserts and conflicts
        id_res = {}
        for ir in result:
            try: del ir['rev']
            except KeyError: pass
            id_res[ir['id']] = ir

        return_data = {}
        for record_id, id_ in idmap.items():
            return_data[record_id] = id_res[id_]

        defer.returnValue(return_data)


    @defer.inlineCallbacks
    def getUsageRecord(self, record_id):

        doc = yield self.db.retrieveDocument(record_id)
        defer.returnValue(json.dumps(doc))


    @defer.inlineCallbacks
    def getView(self, view_name):

        try:
            view_def = self.views[view_name]
        except KeyError:
            raise InvalidViewError('No such view, %s' % view_name)

        startkey, endkey = view_def.filter()
        doc = yield self.db.queryView(view_def.db_design, view_def.db_view,
                                      startkey=startkey, endkey=endkey)
        rows = convert.viewResultToRows(doc)
        rows = view_def.process(rows)
        defer.returnValue((rows, view_def.description))


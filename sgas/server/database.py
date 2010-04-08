"""
High level database access which incorportes usage records.
"""
import json

from twisted.python import log
from twisted.internet import defer

from sgas.server import usagerecord, convert
from sgas.viewengine import chunkprocess



class InvalidViewError(Exception):
    """
    Raised when the specified view does not exist
    """
    pass



class UsageRecordDatabase:

    def __init__(self, db, info_chunks=None, custom_views=None):
        self.db = db
        self.info_chunks = info_chunks
        self.custom_views = custom_views or {}


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


    # stock views

    def getViewAttributeList(self, attribute):

        if self.info_chunks is None:
            return defer.fail(InvalidViewError('No stock view information available'))

        d = self.info_chunks.getInformationChunks()
        d.addCallback(lambda chunks : set( [ c.get(attribute) for c in chunks ] ) )
        return d


    def viewQuery(self, query_options):
        # group      : attr
        # cluster    : attr
        # filter     : { attr : value }
        # resolution : { attr : level }
        # values     : [ attr1, attr2 ]

        d = self.info_chunks.getInformationChunks()
        d.addCallback(chunkprocess.chunkQuery, query_options)
        return d

    # custom views

    def getCustomViewList(self):

        return self.custom_views.keys()


    def getCustomViewData(self, view_name):

        def gotResult(doc):
            rows = convert.viewResultToRows(doc)
            rows = view_def.process(rows)
            return rows, view_def.description

        try:
            view_def = self.custom_views[view_name]
        except KeyError:
            raise InvalidViewError('No such view, %s' % view_name)

        startkey, endkey = view_def.filter()
        d = self.db.queryView(view_def.db_design, view_def.db_view, startkey=startkey, endkey=endkey)
        d.addCallback(gotResult)
        return d


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


    def viewQuery(self, group, cluster=None, filter=None, resolution=None, values=None):
        # group      : attr
        # cluster    : attr
        # filter     : { attr : value }
        # resolution : { attr : level }
        # values     : [ attr1, attr2 ]

        def removeChunkAttributes(chunk, attributes):
            cc = chunk.copy()
            for attr in attributes:
                cc.pop(attr, None)
            return cc

        def chunkDate(chunk):
            date = None
            try:
                date  = chunk['year']
                date += ' ' + chunk['month']
                date += ' ' + chunk['day']
            except KeyError:
                pass
            return date

        def filterResults(chunks):
#            print "CHUNKS PRE-FILTER", len(chunks)
            filtered_chunks = []
            for chunk in chunks:
                for attr, value in filter.items():
                    if chunk.get(attr) != value:
                        break
                    cc = removeChunkAttributes(chunk, filter.keys())
                    filtered_chunks.append(cc)
#            print "CHUNKS POST-FILTER", len(filtered_chunks)
            return filtered_chunks

        def changeResolution(chunks):
            # allows different scope in vo and time
            # this is really just deleting the "right" information and resumming (done later)
#            print "BEFORE RES", chunks
            del_attrs = []
            vo_res    = resolution.get('vo', 2)
            date_res  = resolution.get('date', 2)
            if vo_res   <= 2: del_attrs.append('vo_role')
            if vo_res   <= 1: del_attrs.append('vo_group')
            if vo_res   <= 0: del_attrs.append('vo')
            if date_res <= 2: del_attrs.append('day')
            if date_res <= 1: del_attrs.append('month')
            if date_res <= 0: del_attrs.append('year')

            new_chunks = [ removeChunkAttributes(chunk, del_attrs) for chunk in chunks ]
#            print "AFTER RES", new_chunks
            return new_chunks

        def groupResults(chunks):
#            print "GROUPING", chunks
            grouped_results = {}
            for chunk in chunks:
                group_attr = chunk.get(group)
                group_date = chunkDate(chunk)
                key = (group_attr, group_date)
                cc = removeChunkAttributes(chunk, [group] + ['year', 'month', 'day'])
                grouped_results.setdefault(key, []).append(cc)
#            print "GROUPED", grouped_results
            return grouped_results.items()

        def sumChunks(chunks, values):
#            print "SUMMING CHUNKS", chunks
            ccw_values = [ [ chunk.pop(value) for value in values ] for chunk in chunks ]
            summed_chunks = [ sum(values) for values in zip(*ccw_values) ]
#            print "SUMMED_CHUNKS", summed_chunks
            return summed_chunks

        def sumGroups(grouped_results, values):
            summed_grouped_results = [ (group, sumChunks(chunks, values)) for group, chunks in grouped_results ]
            return summed_grouped_results

        if values is None:
            values = ['count', 'cputime', 'walltime']

        d = self.info_chunks.getInformationChunks()
        if filter     : d.addCallback(filterResults)
        if resolution : d.addCallback(changeResolution)
        d.addCallback(groupResults)
        d.addCallback(sumGroups, values)
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


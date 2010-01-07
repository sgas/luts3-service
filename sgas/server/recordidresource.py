"""
UR insertion resource for VTAS.

Used for inserting usage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

from twisted.python import log
from twisted.web import resource, server



class RecordIDResource(resource.Resource):

    isLeaf = True

    def __init__(self, database):
        resource.Resource.__init__(self)
        self.database = database


    def render_GET(self, request):

        def gotDocument(doc):
            if doc:
                request.write(doc)
            else:
                request.setResponseCode(404)
                request.write('No such document')
            request.finish()

        #print "GET recordId", request.prepath, request.postpath
        if request.postpath and len(request.postpath) == 1:
            doc_id = request.postpath[0]
            log.msg("Request for retrieving document %s" % doc_id, system='sgas.RecordIDResource')
            d = self.database.getUsageRecord(doc_id)
            d.addCallback(gotDocument)
            return server.NOT_DONE_YET
        else:
            # invalid path, 
            log.msg("Invalid path while requesting for record", system='sgas.RecordIDResource')
            request.setResponseCode(400)
            return 'Invalid path'


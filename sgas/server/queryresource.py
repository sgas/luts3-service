"""
Query resource for SGAS.

Used for querying for and retrieving aggregated accounting information.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.authz import rights
from sgas.server import resourceutil
from sgas.queryengine import parser as queryparser, builder as querybuilder, rowrp as queryrowrp



class QueryResource(resource.Resource):

    isLeaf = True

    def __init__(self, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db
        self.authorizer = authorizer


    def queryDatabase(self, query_args):

        query, query_args = querybuilder.buildQuery(query_args)
        d = self.db.query(query, query_args)
        return d


    def render_GET(self, request):

        #print request.path, request.args
        try:
            query_args = queryparser.parseURLArguments(request.args)
        except queryparser.QueryParseError, e:
            request.setResponseCode(400) # bad request
            log.msg('Rejecting query request: %s' % str(e))
            return str(e)

        authz_params = queryparser.filterAuthzParams(query_args)

        # need to add context sometime
        subject = resourceutil.getSubject(request)
        if not self.authorizer.isAllowed(subject, rights.ACTION_QUERY, context=authz_params.items()):
            request.setResponseCode(403) # forbidden
            return "Query not allowed for given context for identity %s" % subject
        # request allowed, continue

        # hostname is used for logging / provenance in the usage records
        hostname = request.getClient()
        if hostname is None:
            hostname = request.getClientIP()
        log.msg('Accepted query request from %s' % hostname, system='sgas.queryresource')


        def gotDatabaseResult(rows):
            records = queryrowrp.buildDictRecords(rows, query_args)
            payload = json.dumps(records)
            request.write(payload)
            request.finish()

        def queryError(error):
            log.msg('Queryengine error: %s' % str(error))
            log.msg('Queryengine error args' % str(query_args))
            request.setResponseCode(500)
            request.write('Queryengine error (%s)' % str(error))
            request.finish()

        def resultHandlingError(error):
            log.msg('Query result error: %s' % str(error))
            log.msg('Query result error args' % str(query_args))
            request.setResponseCode(500)
            request.write('Query result error (%s)' % str(error))
            request.finish()

        d = self.queryDatabase(query_args)
        d.addCallbacks(gotDatabaseResult, queryError)
        d.addErrback(resultHandlingError)
        return server.NOT_DONE_YET


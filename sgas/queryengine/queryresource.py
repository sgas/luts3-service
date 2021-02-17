"""
Query resource for SGAS.

Used for querying for and retrieving aggregated accounting information.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.authz import rights, ctxsetchecker
from sgas.server import resourceutil
from sgas.queryengine import parser as queryparser, builder as querybuilder, rowrp as queryrowrp



JSON_MIME_TYPE = 'application/json'
HTTP_HEADER_CONTENT_TYPE   = 'content-type'

ACTION_QUERY        = 'query'
CTX_MACHINE_NAME    = 'machine_name'
CTX_USER_IDENTITY   = 'user_identity'
CTX_VO_NAME         = 'vo_name'

class QueryResource(resource.Resource):
    
    PLUGIN_ID   = 'query'
    PLUGIN_NAME = 'Query' 

    isLeaf = True

    def __init__(self, cfg, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db
        self.authorizer = authorizer
        
        authorizer.addChecker(ACTION_QUERY, ctxsetchecker.AllSetChecker)
        authorizer.rights.addActions(ACTION_QUERY)
        authorizer.rights.addOptions(ACTION_QUERY,[rights.OPTION_ALL])
        authorizer.rights.addContexts(ACTION_QUERY,[CTX_MACHINE_NAME, CTX_USER_IDENTITY, CTX_VO_NAME])


    def queryDatabase(self, query_args):
        query, query_args = querybuilder.buildQuery(query_args)
        d = self.db.query(query, query_args)
        return d


    def render_GET(self, request):
        try:
            query_args = queryparser.parseURLArguments(request.args)
        except queryparser.QueryParseError as e:
            request.setResponseCode(400) # bad request
            log.msg('Rejecting query request: %s' % str(e), system='sgas.QueryResource')
            return str(e)

        authz_params = queryparser.filterAuthzParams(query_args)

        subject = resourceutil.getSubject(request)
        if not self.authorizer.isAllowed(subject, ACTION_QUERY, context=authz_params.items()):
            request.setResponseCode(403) # forbidden
            return "Query not allowed for given context for identity %s" % subject
        # request allowed, continue

        hostname = resourceutil.getHostname(request)
        log.msg('Accepted query request from %s' % hostname, system='sgas.QueryResource')

        def gotDatabaseResult(rows):
            records = queryrowrp.buildDictRecords(rows, query_args)
            payload = json.dumps(records)
            request.setHeader(HTTP_HEADER_CONTENT_TYPE, JSON_MIME_TYPE)
            request.write(payload)
            request.finish()

        def queryError(error):
            log.msg('Queryengine error: %s' % str(error), system='sgas.QueryResource')
            log.msg('Queryengine error args' % str(query_args), system='sgas.QueryResource')
            request.setResponseCode(500)
            request.write('Queryengine error (%s)' % str(error), system='sgas.QueryResource')
            request.finish()

        def resultHandlingError(error):
            log.msg('Query result error: %s' % str(error), system='sgas.QueryResource')
            log.msg('Query result error args' % str(query_args), system='sgas.QueryResource')
            request.setResponseCode(500)
            request.write('Query result error (%s)' % str(error), system='sgas.QueryResource')
            request.finish()

        d = self.queryDatabase(query_args)
        d.addCallbacks(gotDatabaseResult, queryError)
        d.addErrback(resultHandlingError)
        return server.NOT_DONE_YET


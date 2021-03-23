"""
Custom Query resource for SGAS.

Used for querying for and retrieving aggregated accounting information.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.authz import rights as authrights, ctxsetchecker
from sgas.customqueryengine import rights
from sgas.server import resourceutil
from sgas.customqueryengine import querydefinition


JSON_MIME_TYPE = 'application/json'
HTTP_HEADER_CONTENT_TYPE   = 'content-type'

ACTION_CUSTOMQUERY  = 'customquery'

class QueryResource(resource.Resource):
    
    PLUGIN_ID   = 'customquery'
    PLUGIN_NAME = 'CustomQuery'

    isLeaf = True

    def __init__(self, cfg, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db
        self.authorizer = authorizer        
        self.queries = querydefinition.buildQueryList(cfg)
        
        self.authorizer.addChecker(ACTION_CUSTOMQUERY, ctxsetchecker.AnySetChecker)
        self.authorizer.rights.addActions(ACTION_CUSTOMQUERY)
        self.authorizer.rights.addOptions(ACTION_CUSTOMQUERY,[authrights.OPTION_ALL])


    def queryDatabase(self, query, query_args):
        return self.db.dictquery(query, query_args)


    def render_GET(self, request):
        if not len(request.postpath) == 1:
            request.setResponseCode(400) # bad request
            log.msg('Missing query name', system='sgas.QueryResource')
            return "Missing query name"

        if not request.postpath[0]:
            request.setResponseCode(400) # bad request
            log.msg('Missing query name', system='sgas.QueryResource')
            return "Missing query name"

        query_name = request.postpath[0].decode('utf-8')               

        query = None
        for q in self.queries:
            if q.query_name == query_name:
                query = q
                break

        if not query:
            request.setResponseCode(400) # bad request
            log.msg('Query "%s" does not exist' % query_name, system='sgas.QueryResource')
            return "Query does not exist"

        try:
            query_args = query.parseURLArguments(request.args)
        except querydefinition.QueryParseError as e:
            request.setResponseCode(400) # bad request
            log.msg('Rejecting custom query request: %s' % str(e), system='sgas.QueryResource')
            return str(e)
        
        ctx = [ (rights.CTX_QUERY, query_name) ] + [ (rights.CTX_QUERYGROUP, vg) for vg in query.query_group ]

        subject = resourceutil.getSubject(request)
        
        if not self.authorizer.isAllowed(subject, ACTION_CUSTOMQUERY, context=ctx):
            request.setResponseCode(403) # forbidden
            return ("CustomQuery not allowed for given context for identity %s" % subject).encode('utf-8')
        # request allowed, continue

        hostname = resourceutil.getHostname(request)
        log.msg('Accepted query request from %s' % hostname, system='sgas.QueryResource')

        def gotDatabaseResult(rows):
            payload = json.dumps(rows).encode('utf-8')
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

        d = self.queryDatabase(query.query,query_args)
        d.addCallbacks(gotDatabaseResult, queryError)
        d.addErrback(resultHandlingError)
        return server.NOT_DONE_YET


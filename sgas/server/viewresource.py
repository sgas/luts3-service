"""
UsageRecord view resource.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.database import error as dberror
from sgas.authz import rights
from sgas.server import resourceutil
from sgas.viewengine import pagebuilder


JSON_MIME_TYPE = 'application/json'
HTML_MIME_TYPE = 'text/html'

HTTP_HEADER_CONTENT_LENGTH = 'content-length'
HTTP_HEADER_CONTENT_TYPE   = 'content-type'


HTML_HEADER = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>%(title)s</title>
        <link rel="stylesheet" type="text/css" href="/static/css/view.frontpage.css" />
    </head>
    <body>
"""

HTML_FOOTER = """   </body>
</html>
"""

HTML_VIEW_HEADER = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>%(title)s</title>
        <link rel="stylesheet" type="text/css" href="/static/css/view.table.css" />
        <script type="text/javascript" src="/static/js/protovis-d3.1.js"></script>
    </head>
    <body>
"""

HTML_VIEW_FOOTER = HTML_FOOTER




def getReturnMimeType(request):
    # given a request, figure out if json or html content should be returned
    # json is default
    accepts = request.requestHeaders.getRawHeaders('Accept', [])
    return_type = 'json'
    if accepts:
        for acc in accepts[0].split(','):
            if acc == JSON_MIME_TYPE:
                return_type = 'json'
                break
            elif acc == HTML_MIME_TYPE:
                return_type = 'html'
                break
    return return_type



# generic error handler
def handleViewError(error, request, view_name):
    error_msg = error.getErrorMessage()
    if error.check(dberror.DatabaseUnavailableError):
        error.printTraceback()
        log.err(error)
        request.setResponseCode(503)
        error_msg = 'Database is currently unavailable, please try again later'
    else:
        log.err(error)
        request.setResponseCode(500)

    request.write(error_msg)
    request.finish()



class ViewTopResource(resource.Resource):

    def __init__(self, urdb, authorizer, views):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer

        self.views = views

        for view in self.views:
            self.putChild(view.view_name, GraphRenderResource(view, urdb, authorizer))


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)
        return self.renderStartPage(request, subject)


    def renderStartPage(self, request, identity):

        ib = 4 * ' '

        body =''
        body += 2*ib + '<h3>SGAS View Page</h3>\n'
        body += 2*ib + '<p>\n'
        body += 2*ib + '<div>Identity: %(identity)s</div>\n' % {'identity': identity }
        body += 2*ib + '<p>\n'
        for view in self.views:
            body += 2*ib + '<div><a href=view/%s>%s</a></div>\n' % (view.view_name, view.caption)
        if not self.views:
            body += 2*ib + '<div>No views defined in configuration file. See docs/views in the documentation for how specify views.</div>\n'

        request.write(HTML_HEADER % {'title': 'View startpage'} )
        request.write(body)
        request.write(HTML_FOOTER)
        #request.setResponseCode(200)
        request.finish()

        return server.NOT_DONE_YET



class GraphRenderResource(resource.Resource):

    def __init__(self, view, urdb, authorizer):
        resource.Resource.__init__(self)
        self.view = view
        self.urdb = urdb
        self.authorizer = authorizer


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)
        # authZ check
        ctx = [ (rights.CTX_VIEW, self.view.view_name) ] + [ (rights.CTX_VIEWGROUP, vg) for vg in self.view.view_groups ]
        if self.authorizer.isAllowed(subject, rights.ACTION_VIEW, context=ctx):
            return self.renderView(request)

        # access not allowed
        request.write('<html><body>Access to view %s not allowed for %s</body></html>' % (self.view.view_name, subject))
        request.finish()
        return server.NOT_DONE_YET


    def renderView(self, request):

        def gotResult(rows, return_type):
            return_type = 'html'
            if return_type == 'json':
                retval = json.dumps(rows)
                request.responseHeaders.setRawHeaders(HTTP_HEADER_CONTENT_TYPE, [JSON_MIME_TYPE])
                request.responseHeaders.setRawHeaders(HTTP_HEADER_CONTENT_LENGTH, [str(len(retval))])
                request.write(json.dumps(rows))

            elif return_type == 'html':
                # twisted web sets content-type to text/html per default
                page_body = pagebuilder.buildViewPage(self.view, rows)

                request.write(HTML_VIEW_HEADER % {'title': self.view.caption} )
                request.write(page_body)
                request.write(HTML_VIEW_FOOTER)

            else:
                request.setResponseCode(500)
                request.write('<html><body>Something went wrong when choosing return type</body></html>')

            request.finish()

        return_type = getReturnMimeType(request)

        d = self.urdb.query(self.view.query)
        d.addCallback(gotResult, return_type)
        d.addErrback(handleViewError, request, self.view.view_name)
        return server.NOT_DONE_YET


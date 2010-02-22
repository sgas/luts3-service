"""
UsageRecord view resource.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

import json

from twisted.python import log
from twisted.web import resource, server

from sgas.server import authz, convert, database, couchdb


JSON_MIME_TYPE = 'application/json'
HTML_MIME_TYPE = 'text/html'

HTTP_HEADER_CONTENT_LENGTH = 'content-length'
HTTP_HEADER_CONTENT_TYPE   = 'content-type'


HTML_HEADER = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>%(title)s</title>
        <link rel="stylesheet" type="text/css" href="/static/css/view.visualize.css" />
        <script type="text/javascript" src="/static/js/jquery-1.3.2.min.js"></script>
        <script type="text/javascript" src="/static/js/visualize.jQuery.js"></script>
        <script type="text/javascript">
            jQuery(function(){
                //make some charts
                $('table').visualize({type: 'line', height: 300, title: '%(title)s'});
                });
        </script>
    </head>
    <body>
"""

HTML_FOOTER = """   </body>
</html>
"""



class ViewResource(resource.Resource):

    isLeaf = True

    def __init__(self, urdb, authorizer):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer


    def render_GET(self, request):

        def gotResult((rows, view_description), return_type, view_name):
            if return_type == 'json':
                retval = json.dumps(rows)
                request.responseHeaders.setRawHeaders(HTTP_HEADER_CONTENT_TYPE, [JSON_MIME_TYPE])
                request.responseHeaders.setRawHeaders(HTTP_HEADER_CONTENT_LENGTH, [str(len(retval))])
                request.write(json.dumps(rows))
            elif return_type == 'html':
                html_table = convert.rowsToHTMLTable(rows, caption=view_description)
                # twisted web sets content-type to text/html per default
                request.write(HTML_HEADER % {'title': view_description} )
                request.write(str(html_table))
                request.write(HTML_FOOTER)
            else:
                request.setResponseCode(500)
                request.write('''"Something went wrong when choosing return type"''')

            request.finish()

        def viewError(error, return_type, view_name):
            error_msg = error.getErrorMessage()
            if error.check(database.InvalidViewError):
                request.setResponseCode(404)
                request.write(error.getErrorMessage())
            elif error.check(couchdb.InvalidViewError):
                log.msg('Error accessing view specified in view definition %s' % view_name, system='sgas.ViewResource')
                log.msg('This is probably an error in the configuration or a view that has not been created', system='sgas.ViewResource')
                log.err(error)
                request.setResponseCode(500)
                error_msg = 'Error accessing view in database (punch the admin)'
            elif error.check(couchdb.DatabaseUnavailableError):
                error.printTraceback()
                log.err(error)
                request.setResponseCode(503)
                error_msg = 'Database is currently unavailable, please try again later'
            else:
                log.err(error)
                request.setResponseCode(500)

            request.write(error_msg)
            request.finish()
        # def viewError

        # first, extract view name
        if len(request.postpath) == 1 or (len(request.postpath) == 2 and request.postpath[1] == ''):
            view_name = request.postpath[0]
        else:
            request.setResponseCode(404)
            return 'Unknown resource'

        subject = authz.getSubject(request)
        if not self.authorizer.isAllowed(subject, authz.VIEW, view_name):
            request.setResponseCode(403) # forbidden
            return "Access to view %s not allowed for %s" % (view_name, subject)

        # figure out if we should return json or html content, json is default
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

        d = self.urdb.getView(view_name)
        d.addCallbacks(gotResult, viewError,
                       callbackArgs=(return_type, view_name),
                       errbackArgs=(return_type, view_name))
        return server.NOT_DONE_YET


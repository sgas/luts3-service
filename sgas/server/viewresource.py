"""
UsageRecord view resource.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

import json
import urllib

from twisted.python import log
from twisted.web import resource, server

from sgas.common import couchdb
from sgas.server import authz, convert, database, viewresourcehelper


JSON_MIME_TYPE = 'application/json'
HTML_MIME_TYPE = 'text/html'

HTTP_HEADER_CONTENT_LENGTH = 'content-length'
HTTP_HEADER_CONTENT_TYPE   = 'content-type'


HTML_HEADER = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <title>%(title)s</title>
        <link rel="stylesheet" type="text/css" href="/static/css/view.css" />
    </head>
    <body>
"""

HTML_FOOTER = """   </body>
</html>
"""



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
    if error.check(database.InvalidViewError):
        request.setResponseCode(404)
        request.write(error.getErrorMessage())
    elif error.check(couchdb.InvalidViewError):
        log.msg('Error accessing view specified in view definition %s' % view_name, system='sgas.viewresource')
        log.msg('This is probably an error in the configuration or a view that has not been created', system='sgas.viewresource')
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



class ViewTopResource(resource.Resource):

    def __init__(self, urdb, authorizer):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer

        self.stock_views = ['user', 'host', 'vo']
        for va in self.stock_views:
            self.putChild(va, StockViewResource(va, urdb, authorizer))
        self.putChild('custom', CustomViewTopResource(urdb, authorizer))


    def render_GET(self, request):
        subject = authz.getSubject(request)
        self.renderStartPage(request, subject)


    def renderStartPage(self, request, identity):

        custom_views = self.urdb.getCustomViewList()
        allowed_actions = self.authorizer.getAllowedActions(identity)
        ib = 4 * ' '

        body =''
        body += 2*ib + '<div>Hello %(identity)s</div>\n' % {'identity': identity }
        body += 2*ib + '<div>Allowed actions: %(actions)s</div>\n' % {'actions': ''.join(allowed_actions) }
        body += 2*ib + '<h2>Stock views</h2>\n'
        for sv in self.stock_views:
            body += 2*ib + '<div><a href=view/%(view_name)s>%(view_name)s</a></div>\n' % {'view_name': sv }
        body += 2*ib + '<h2>Custom views</h2>\n'
        for cv in custom_views:
            body += 2*ib + '<div><a href=view/custom/%(view_name)s>%(view_name)s</a></div>\n' % {'view_name': cv }

        request.write(HTML_HEADER % {'title': 'View startpage'} )
        request.write(body)
        request.write(HTML_FOOTER)

        request.setResponseCode(200)
        request.finish()




class StockViewResource(resource.Resource):

    def __init__(self, base_attribute, urdb, authorizer):
        resource.Resource.__init__(self)
        self.base_attribute = base_attribute
        self.urdb = urdb
        self.authorizer = authorizer


    def getChild(self, path, request):
        # request for specific subject listing
        #print "STOCK CHILD", self.base_attribute, path
        subject = authz.getSubject(request)
        view_resource = path

        if not self.authorizer.isAllowed(subject, authz.VIEW, self.base_attribute, context=view_resource):
            request.setResponseCode(403) # forbidden
            return "Access to view %s not allowed for %s" % (self.base_attribute, subject)
        else:
            return StockViewSubjectRenderer(self.urdb, self.base_attribute, view_resource)


    def render_GET(self, request):
        # request for resource list

        #postpath = request.postpath
        subject = authz.getSubject(request)
        #print "STOCK", request.prepath, request.postpath

        if not self.authorizer.isAllowed(subject, authz.VIEW, self.base_attribute):
            request.setResponseCode(403) # forbidden
            return "Listing for view %s not allowed for %s" % (self.base_attribute, subject)
        else:
            return self.renderViewList(request)


    def renderViewList(self, request):

        def buildView(viewdata, return_type):
            if return_type == 'json':
                sorted_viewdata = list(sorted(viewdata))
                return_data = json.dumps(sorted_viewdata)
                request.responseHeaders.setRawHeaders(HTTP_HEADER_CONTENT_TYPE, [JSON_MIME_TYPE])
                request.responseHeaders.setRawHeaders(HTTP_HEADER_CONTENT_LENGTH, [str(len(return_data))])
                request.write(json.dumps(return_data))
            elif return_type == 'html':
                if None in viewdata:
                    viewdata.remove(None)
                table_input = sorted(viewdata)
                html_table = convert.createLinkedHTMLTableList(table_input,
                                                               prefix=self.base_attribute + '/',
                                                               caption="%s list" % self.base_attribute.capitalize())
                # twisted web sets content-type to text/html per default
                request.write(HTML_HEADER % {'title': 'View enumeration'} )
                request.write(str(html_table))
                request.write(HTML_FOOTER)
            else:
                request.setResponseCode(500)
                request.write('''"Something went wrong when choosing return type"''')

            request.finish()

        #print "RENDER VIEW LIST", self.base_attribute
        return_type = getReturnMimeType(request)

        d = self.urdb.getViewAttributeList(self.base_attribute)
        d.addCallback(buildView, return_type)
        d.addErrback(handleViewError, request, '%s listing' % self.base_attribute.capitalize())
        return server.NOT_DONE_YET



class StockViewSubjectRenderer(resource.Resource):

    GROUPS = ['user', 'host', 'vo']
    DATE_RESOLUTIONS = { 'collapse':0, 'year':1, 'month':2, 'day':3 }
    VO_RESOLUTIONS = [ 0, 1, 2 ]


    def __init__(self, urdb, base_attribute, view_resource):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.base_attribute = base_attribute
        self.view_resource = view_resource


    def render_GET(self, request):
        return self.renderView(request)


    def renderView(self, request):

        def createViewOptions(basepath, url_options):
            i8 = 8 * ' '
            lines = []

            HREF_BASE = "<a href=%(url)s>%(name)s</a>"
            OPTION_BASE = "<div>%(description)s: %(hrefs)s (current: %(current)s)</div>"

            createHref = lambda options, name : HREF_BASE % {'url': basepath + '?%s' % urllib.urlencode(options), 'name': name }

            option_order = [ 'group', 'timeres' ]
            descriptions = {
                'group'    : 'Group by',
                'timeres'  : 'Time resolution'
            }
            query_options = {
                'group'   : [ group for group in self.GROUPS if group != self.base_attribute ],
                'timeres' : self.DATE_RESOLUTIONS.keys()
            }

            href_frontpage = HREF_BASE % {'url' : basepath.rsplit('/',2)[0], 'name': 'View frontpage'}
            lines.append("<div>%s</div>" % href_frontpage)

            #print "OPTIONS", basepath, url_options

            for q_option in option_order:
                group_hrefs = []
                for option_value in query_options.get(q_option):
                    if option_value == url_options.get(q_option):
                        continue
                    options = { q_option : option_value }
                    options.update( [ (g,v) for g,v in url_options.items() if g != q_option ] )
                    #print options

                    group_hrefs.append( createHref(options, option_value) )

                shrefs = ' '.join(group_hrefs)

                line = OPTION_BASE % {
                    'description' : descriptions.get(q_option),
                    'hrefs': shrefs,
                    'current': url_options.get(q_option)
                }
                lines.append(line)

            print lines
            return '\n'.join( [ i8 + line for line in lines ] )


        def buildTables(query_result, url_options):
            #print "QUERY_RESULTS:", len(query_result)

            page_title = '%s view for %s' % (self.base_attribute.capitalize(), self.view_resource)

            href_options = createViewOptions(request.path, url_options)
            html_table = convert.rowsToHTMLTable(query_result, caption=page_title)

            request.write(HTML_HEADER % {'title': page_title } )
            request.write(href_options)
            request.write("<div>&nbsp;</div>") # create some vertical sapce
            request.write(html_table)
            request.write(HTML_FOOTER)
            request.finish()

        print "RENDER VIEW", self.base_attribute, self.view_resource, request.args
        # get parameters

        url_options = viewresourcehelper.parseURLParameters(request.args)
        print "URL O", url_options

        query_options = viewresourcehelper.createQueryOptions(url_options, self.base_attribute, self.view_resource)

        d = self.urdb.viewQuery(query_options)
        d.addCallback(buildTables, url_options)
        d.addErrback(handleViewError, request, '%s/%s' % (self.base_attribute, self.view_resource))
        return server.NOT_DONE_YET



class CustomViewTopResource(resource.Resource):


    def __init__(self, urdb, authorizer):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer


    def getChild(self, path, request):
        # request for custom view
        print "CUSTOM getChild", path, request
        subject = authz.getSubject(request)

        view_name = path
        if not self.authorizer.isAllowed(subject, authz.VIEW, view_name):
            request.setResponseCode(403) # forbidden
            return "Access to view %s not allowed for %s" % (view_name, subject)
        else:
            return CustomViewResourceRenderer(self.urdb, view_name)


    def render_GET(self, request):
        print "CUSTOM TOP", request.prepath, request.postpath
        request.setResponseCode(404)
        return 'Please specify specific view resource'



class CustomViewResourceRenderer(resource.Resource):

    def __init__(self, urdb, view_name):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.view_name = view_name


    def render_GET(self, request):
        return self.renderView(request)


    def renderView(self, request):

        def gotResult((rows, view_description), return_type):
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

        return_type = getReturnMimeType(request)

        d = self.urdb.getCustomViewData(self.view_name)
        d.addCallback(gotResult, return_type)
        d.addErrback(handleViewError, request, 'custom/%s' % self.view_name)
        return server.NOT_DONE_YET


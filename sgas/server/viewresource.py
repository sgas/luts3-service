"""
UsageRecord view resource.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import json

from twisted.python import log
from twisted.web import resource, server

from sgas.database import error as dberror
from sgas.server import authz
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
        <link rel="stylesheet" type="text/css" href="/static/css/view.css" />
        <script type="text/javascript" src="/static/js/protovis-d3.1.js"></script>
    </head>
    <body>
"""

HTML_VIEW_FOOTER = HTML_FOOTER



class View:

    def __init__(self, view_type, resource_name, caption, query):
        self.view_type = view_type
        self.caption = caption
        self.resource_name = resource_name
        self.query = query


top_users_sql = """SELECT execution_time, user_identity, sum_walltime FROM (
    SELECT execution_time, user_identity, sum(walltime) AS sum_walltime, rank() OVER (PARTITION BY execution_time ORDER BY sum(walltime) DESC) as rank
    FROM uraggregated GROUP BY execution_time, user_identity ORDER BY execution_time, sum(walltime) DESC) as q
WHERE q.rank <= 3 and q.sum_walltime > 0 and execution_time > (current_date - interval '20 days');"""
#WHERE q.rank <= 3 and q.sum_walltime > 0;"""

VO_HOST_WALLTIME_QUERY = """
SELECT execution_time, COALESCE(vo_name, 'N/A'), (sum(walltime) / 24.0)::integer
FROM uraggregated WHERE execution_time > (current_date - interval '250 days')
GROUP BY execution_time, COALESCE(vo_name, 'N/A');
"""

inserts_view = View(view_type='stacked_bars', resource_name='inserts', caption='Inserted records per day / host',
                    query="SELECT insert_time, machine_name, sum(n_jobs) FROM uraggregated WHERE insert_time > (current_date - interval '20 days') GROUP BY insert_time, machine_name;")
#                    query="SELECT insert_time, machine_name, sum(n_jobs) FROM uraggregated GROUP BY insert_time, machine_name;")

machine_walltime_view = View(view_type='stacked_bars', resource_name='machine_walltime', caption='Aggregated walltime hours per day / host',
                             query="SELECT execution_time, machine_name, sum(walltime) FROM uraggregated WHERE execution_time > (current_date - interval '10 days') GROUP BY execution_time, machine_name;")
#                             query="SELECT execution_time, machine_name, sum(walltime) FROM uraggregated GROUP BY execution_time, machine_name;")

user_walltime_view = View(view_type='stacked_bars', resource_name='user_walltime', caption='Top 3 users per day (aggregated walltime)',
                          #query="SELECT execution_time, user_identity, sum(walltime) FROM uraggregated GROUP BY execution_time, user_identity ORDER BY execution_time, sum(walltime) DESC limit 15;")
                          query=top_users_sql)

vo_host_walltime = View(view_type='stacked_bars', resource_name='vo_host_walltime', caption='Aggregated walltime days per VO',
                        query=VO_HOST_WALLTIME_QUERY)


executed_total = View(view_type='bars', resource_name='executed_longs', caption='Total Job Walltime Days, Last 150 days',
                        query="SELECT execution_time, (sum(walltime) / 24.0)::integer FROM uraggregated " + \
                              "WHERE execution_time > current_date - interval '150 days' GROUP BY execution_time ORDER BY execution_time;")

inserts_total = View(view_type='bars', resource_name='inserts_long', caption='Total Inserts, Last 150 days',
                        query="SELECT insert_time, sum(n_jobs) FROM uraggregated " + \
                              "WHERE insert_time > current_date - interval '150 days' GROUP BY insert_time ORDER BY insert_time;")

VIEWS = [ inserts_view, machine_walltime_view, user_walltime_view, vo_host_walltime, executed_total, inserts_total ]



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

    def __init__(self, urdb, authorizer):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer

        for view in VIEWS:
            self.putChild(view.resource_name, GraphRenderResource(view, urdb, authorizer))


    def render_GET(self, request):
        subject = authz.getSubject(request)
        return self.renderStartPage(request, subject)


    def renderStartPage(self, request, identity):

        ib = 4 * ' '

        body =''
        body += 2*ib + '<div>Hello %(identity)s</div>\n' % {'identity': identity }
        body += 2*ib + '<h2>Views</h2>\n'
        for view in VIEWS:
            body += 2*ib + '<div><a href=view/%s>%s</a></div>\n' % (view.resource_name, view.caption)

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
        return self.renderView(request)


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
        d.addErrback(handleViewError, request, self.view.resource_name)
        return server.NOT_DONE_YET


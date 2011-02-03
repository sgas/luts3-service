"""
Basic view functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.internet import defer
from twisted.web import server, resource

from sgas.server import httphtml



class BaseView(resource.Resource):


    def __init__(self, urdb, authorizer, manifest):

        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer
        self.manifest = manifest


    def renderAuthzErrorPage(self, request, pagename, subject):

        request.write(httphtml.HTML_BASE_HEADER % {'title': 'Authorization Error'})
        request.write('Access to %s not allowed for %s' % (pagename, subject))
        request.write(httphtml.HTML_BASE_FOOTER)
        request.finish()
        return server.NOT_DONE_YET


    def renderErrorPage(self, error, request):

        request.write('Error rendering page: %s' % str(error))
        request.finish()
        return server.NOT_DONE_YET


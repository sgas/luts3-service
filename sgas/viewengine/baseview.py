"""
Basic view functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.python import failure
from twisted.web import server, resource

from sgas.viewengine import html



class ViewError(Exception):
    """
    Base error class for view errors.
    """



class BaseView(resource.Resource):


    def __init__(self, urdb, authorizer, manifest):

        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer
        self.manifest = manifest


    def renderAuthzErrorPage(self, request, pagename, subject):

        request.write(html.HTML_VIEWBASE_HEADER % {'title': 'Authorization Error'})
        request.write('Access to %s not allowed for %s' % (pagename, subject))
        request.write(html.HTML_VIEWBASE_FOOTER)
        request.finish()
        return server.NOT_DONE_YET


    def renderErrorPage(self, error, request):

        if isinstance(error, failure.Failure):
            error_msg = error.getErrorMessage()
        else:
            error_msg = str(error)

        request.write('Error rendering page: %s' % error_msg)
        request.finish()
        return server.NOT_DONE_YET


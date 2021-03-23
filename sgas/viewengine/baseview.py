"""
Basic view functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.python import failure, log
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

        body = html.HTML_VIEWBASE_HEADER % {'title': 'Authorization Error'}
        body += 'Access to %s not allowed for %s' % (pagename, subject)
        body += html.HTML_VIEWBASE_FOOTER
        request.write(body.encode('utf-8'))
        request.finish()
        return server.NOT_DONE_YET


    def renderErrorPage(self, error, request):

        if isinstance(error, failure.Failure):
            error_msg = error.getErrorMessage()
        else:
            error_msg = str(error)

        log.err(error)
        request.write('Error rendering page: %s' % error_msg)
        request.finish()
        return server.NOT_DONE_YET


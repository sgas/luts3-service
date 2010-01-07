"""
Static resource (for serving static content).

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

from twisted.python import log, filepath
from twisted.web import resource



class StaticResource(resource.Resource):
    """
    Resource for sharing static content.
    Current this means files (css, javascript).

    There is currently no restrictions on who can fetch these.
    """

    isLeaf = True

    def __init__(self, directory):
        resource.Resource.__init__(self)
        # use the twisted filepath class as it only allows downward access
        self.dirpath = filepath.FilePath(directory)


    def render_GET(self, request):

        # basic security checks
        if '..' in request.postpath or '.' in request.postpath:
            request.setResponseCode(404)
            return 'Dots not allowed when requesting static content'
        if len(request.postpath) > 3:
            request.setResponseCode(404)
            return 'Request for static content to deep.'

        # find path
        cp = None
        for pp in request.postpath:
            cp = cp and cp.child(pp) or self.dirpath.child(pp)

        if cp and cp.exists() and cp.isfile():
            # set mime-types
            if cp.basename().endswith('.css'):
                request.setHeader('content-type', 'text/css')
            elif cp.basename().endswith('.js'):
                request.setHeader('content-type', 'text/javascript')
            return cp.getContent()
        else:
            request.setResponseCode(404)
            return 'Request resource does not exist..'


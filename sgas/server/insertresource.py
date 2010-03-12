"""
UR insertion resource for SGAS.

Used for inserting usage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

import json

from twisted.python import log
from twisted.web import resource, server

from sgas.server import authz



class InsertResource(resource.Resource):

    isLeaf = False

    def __init__(self, urdb, authorizer):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer


    def render_POST(self, request):

        def insertDone(result):
            request.write(json.dumps(result))
            request.finish()

        def insertError(error):
            from sgas.common import couchdb
            log.msg("Error during insert: %s" % error.getErrorMessage(), system='sgas.InsertResource')

            error_msg = error.getErrorMessage()
            if error.check(couchdb.DocumentAlreadyExistsError):
                request.setResponseCode(409) # conflict
            elif error.check(couchdb.DatabaseUnavailableError):
                request.setResponseCode(503) # service unavailable
                error_msg = 'Database currently unavailable. Please try again later.'
            else:
                request.setResponseCode(500)

            request.write(error_msg)
            request.finish()

        # FIXME check for postpath, and if any reject request

        subject = authz.getSubject(request)
        if not self.authorizer.isAllowed(subject, authz.INSERT):
            request.setResponseCode(403) # forbidden
            return "Insertion not allowed for identity %s" % subject

        # request allowed, continue

        # hostname is used for logging / provenance in the usage records
        hostname = request.getClient()
        if hostname is None:
            hostname = request.getClientIP()

        request.content.seek(0)
        ur_data = request.content.read()
        d = self.urdb.insertUsageRecords(ur_data, identity=subject, hostname=hostname)
        d.addCallbacks(insertDone, insertError)
        return server.NOT_DONE_YET


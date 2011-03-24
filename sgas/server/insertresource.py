"""
UR insertion resource for SGAS.

Used for inserting usage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.authz import rights
from sgas.server import resourceutil
from sgas.database import inserter, error as dberror



class InsertResource(resource.Resource):

    isLeaf = False

    def __init__(self, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db
        self.authorizer = authorizer


    def render_POST(self, request):

        def insertDone(result):
            request.write(json.dumps(result))
            request.finish()

        def insertError(error):
            log.msg("Error during insert: %s" % error.getErrorMessage(), system='sgas.InsertResource')

            error_msg = error.getErrorMessage()
            if error.check(dberror.DatabaseUnavailableError):
                request.setResponseCode(503) # service unavailable
                error_msg = 'Database currently unavailable. Please try again later.'
            elif error.check(dberror.SecurityError):
                request.setResponseCode(406) # not acceptable
            else:
                request.setResponseCode(500)

            request.write(error_msg)
            request.finish()

        # FIXME check for postpath, and if any reject request

        subject = resourceutil.getSubject(request)
        if not self.authorizer.hasRelevantRight(subject, rights.ACTION_INSERT):
            request.setResponseCode(403) # forbidden
            return "Insertion not allowed for identity %s" % subject

        # request allowed, continue

        # hostname is used for logging / provenance in the usage records
        hostname = resourceutil.getHostname(request)

        request.content.seek(0)
        ur_data = request.content.read()
        d = inserter.insertJobUsageRecords(ur_data, self.db, self.authorizer, subject, hostname)
        d.addCallbacks(insertDone, insertError)
        return server.NOT_DONE_YET


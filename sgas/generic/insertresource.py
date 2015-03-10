"""
Insertion resources for SGAS.

Used for inserting records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
        Magnus Jonsson <magnus@hpc2n.umu.se>
Copyright: NorduNET / Nordic Data Grid Facility (2009, 2010, 2011)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.authz import rights
from sgas.server import resourceutil
from sgas.database import error as dberror



class GenericInsertResource(resource.Resource):

    isLeaf = False

    authz_right = None
    insert_error_msg = 'Error during insert: %s'
    insert_authz_reject_msg = 'Rejecting insert for %s, has no insert rights.'

    def __init__(self, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db
        self.authorizer = authorizer


    def insertRecords(self, data, subject, hostname):
        raise NotImplementedError('This method should have been overridden in subclass')
    
    def render_POST(self, request):

        def insertDone(result):
            request.write(json.dumps(result))
            request.finish()

        def insertError(error):
            #log.msg("Error during insert: %s" % error.getErrorMessage(), system='sgas.InsertResource')
            log.msg(self.insert_error_msg % error.getErrorMessage(), system='sgas.InsertResource')
            log.err(error)

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
        if not self.authorizer.hasRelevantRight(subject, self.authz_right):
            reject_msg = self.insert_authz_reject_msg % subject
            #log.msg("Rejecting insert for %s, has no insert rights." % subject)
            log.msg(reject_msg)
            request.setResponseCode(403) # forbidden
            return reject_msg

        # request allowed, continue

        # hostname is used for logging / provenance in the usage records
        hostname = resourceutil.getHostname(request)

        request.content.seek(0)
        data = request.content.read()
        d = self.insertRecords(data, subject, hostname)
        d.addCallbacks(insertDone, insertError)
        return server.NOT_DONE_YET


"""
Resource for extracting monitoring information from SGAS.

Used for constructing Nagios probes, etc.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

from twisted.python import log, failure
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.authz import rights
from sgas.server import resourceutil
from sgas.queryengine import parser as queryparser, builder as querybuilder, rowrp as queryrowrp



JSON_MIME_TYPE = 'application/json'
HTTP_HEADER_CONTENT_TYPE   = 'content-type'

REGISTRATION_EPOCH = 'registration_epoch'

STATUS_QUERY = """
SELECT extract(EPOCH from (current_timestamp - greatest(ur.last_registration, sr.last_registration)))::integer AS registration_epoch FROM 
  (SELECT max(insert_time) AS last_registration FROM storagerecords where storage_system = %s) AS ur,
  (SELECT max(generate_time) AS last_registration FROM uraggregated WHERE machine_name = %s) AS sr
;
"""



class MonitorResource(resource.Resource):

    isLeaf = True

    def __init__(self, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db
        self.authorizer = authorizer


    def queryStatus(self, machine_name):

        d = self.db.query(STATUS_QUERY, (machine_name,machine_name))
        return d


    def render_GET(self, request):

        #print request.postpath
        subject = resourceutil.getSubject(request)
        if not self.authorizer.isAllowed(subject, rights.ACTION_MONITOR, () ):
            request.setResponseCode(403) # forbidden
            return "Monitoring not allowed for %s" % subject
        # request allowed, continue

        if len(request.postpath) != 1:
            return self.renderErrorPage('Invalid machine specification', request)

        machine_name = request.postpath[0]

        d = self.queryStatus(machine_name)
        d.addCallback(self.renderMonitorStatus, machine_name, request)
        d.addErrback(self.renderErrorPage, request)
        return server.NOT_DONE_YET


    def renderMonitorStatus(self, db_result, machine_name, request):

        if len(db_result) == 0:
            request.setResponseCode(404)
            request.write('No entry for %s' % machine_name)
            request.finish()

        elif len(db_result) == 1:
            reg_epoch = db_result[0][0]
            payload = json.dumps( { 'registration_epoch' : reg_epoch } )
            request.write(payload)
            request.finish()

        else:
            log.msg('Error: Got multiple results for monitor status (should not happen)', system='sgas.MonitorResource')
            log.msg('Database result: %s' % str(db_result), system='sgas.MonitorResource')
            request.setResponseCode(500)
            request.write('Internal error in monitor resource (got multiple results)')
            request.finish()


    def renderErrorPage(self, error, request):

        if isinstance(error, failure.Failure):
            log.err(error)
            error_msg = error.getErrorMessage()
        else:
            error_msg = str(error)

        request.write('Error rendering page: %s' % error_msg)
        request.finish()
        return server.NOT_DONE_YET


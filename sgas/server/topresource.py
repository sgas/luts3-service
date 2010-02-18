"""
Top resource for SGAS.

Used for fetching service urls, and inserting usage records
into the underlying database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

from xml.etree import cElementTree as ET

from twisted.python import log
from twisted.web import resource

from sgas.server import authz


XML_HEADER = '''<?xml version="1.0" encoding="UTF-8"?>'''


class TopResource(resource.Resource):

    def __init__(self, authorizer):
        resource.Resource.__init__(self)
        self.authorizer = authorizer
        self.services = {}


    def registerService(self, resource, resource_path, service_specs):

        for service_name, path_template in service_specs:
            self.services[service_name] = path_template

        self.putChild(resource_path, resource)


    def _createBaseURL(self, host, secure, basepath):
        baseurl = 'http'
        if secure:
            baseurl += 's'
        baseurl += '://' + host + '/' + basepath
        return baseurl


    def _createServiceTree(self, baseurl):
        tree = ET.Element('services')
        for service_name, path_template in self.services.items():
            se = ET.SubElement(tree, 'service')
            se_name = ET.SubElement(se, 'name')
            se_name.text = service_name
            se_href = ET.SubElement(se, 'href')
            se_href.text = baseurl + '/' + path_template
        return tree


    def render_GET(self, request):

        # No authz check, we allow everyone to fetch service list
        subject = authz.getSubject(request)
        log.msg("Request for service list by %s" % subject, system='sgas.TopResource')

        #print "Host Header:", request.received_headers.get('host')
        host = request.received_headers.get('host')
        if not host:
            log.msg('Client did not send proper host header.', system='sgas.TopResource')
            return # FIXME this returns 500...

        basepath = '/'.join(request.prepath)
        baseurl = self._createBaseURL(host, request.isSecure(), basepath)
        #print "BASEURL", baseurl
        tree = self._createServiceTree(baseurl)
        ts = XML_HEADER + ET.tostring(tree) + "\n"
        return ts


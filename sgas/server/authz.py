"""
Small authorization framework.
"""

import re

from twisted.python import log


# various actions
INSERT    = 'insert'
RETRIEVAL = 'retrieval'
VIEW      = 'view'

# regular expression for matching authz lines
rx = re.compile("""\s*"(.*)"\s*(.*)""")


def getSubject(request):
    """
    Utility method for extracting the subject name from a twisted.web.http.Request
    """
    if request.isSecure():
        x509 = request.transport.getPeerCertificate()
        if x509:
            #print x509.get_subject().get_components()
            subject = '/' + '/'.join([ '='.join(c) for c in x509.get_subject().get_components() ])
            return subject

    # identity forwarded by reverse proxy
    if request.getClientIP() == '127.0.0.1' and 'x-ssl-subject' in request.received_headers:
        return request.received_headers.get('x-ssl-subject')

    # request wasn't secure or no certificate was presented
    return None



class Authorizer:

    def __init__(self, authz_file=None):
        self.allowed_actions = {} # subject -> [actions]
        if authz_file is not None:
            self.allowed_actions = self._parseAuthzFile(authz_file)


    def _parseAuthzFile(self, authz_file):
        parse_authz = {}
        for line in open(authz_file).readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            m = rx.match(line)
            if not m:
                continue
            subject, actions_desc = m.groups()
            actions = actions_desc.split(',')
            parse_authz.setdefault(subject, set()).update([ a.strip() for a in actions])
        return parse_authz


    def getAllowedActions(self, subject):
        """
        Returns list of allowed actions for a subject.
        Empty list is returned in case the subject has no entries.
        """
        return self.allowed_actions.get(subject, [])


    def isAllowed(self, subject, action, context=None):

        allowed = False

        try:
            if action in self.allowed_actions[subject]:
                allowed = True
            if context is not None and action + ':' + context in self.allowed_actions[subject]:
                allowed = True
        except KeyError, e:
            pass

        log.msg("Authz check: Subject %s, Action %s, Context: %s. Access allowed: %s" % \
                 (subject, action, context, allowed), system='sgas.Authorizer')

        return allowed


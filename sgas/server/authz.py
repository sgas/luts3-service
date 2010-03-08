"""
Small authorization framework.
"""

import re

from twisted.python import log


# various actions
INSERT    = 'insert'
RETRIEVAL = 'retrieval'
VIEW      = 'view'
VIEW_PREFIX = 'view:'

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


    def isAllowed(self, subject, action, view_name=None, context=None):

        allowed = False

        try:
            if action in self.allowed_actions[subject]:
                allowed = True
            # check views
            elif action == VIEW and view_name:
                if VIEW_PREFIX + view_name in self.allowed_actions[subject]:
                    allowed = True
                elif context is not None and VIEW_PREFIX + view_name + ':' + context in self.allowed_actions[subject]:
                    allowed = True
        except KeyError, e:
            pass

        log.msg("Authz check: Subject %s, Action %s, View name: %s, Context: %s. Access allowed: %s" % \
                 (subject, action, view_name, context, allowed), system='sgas.Authorizer')

        return allowed


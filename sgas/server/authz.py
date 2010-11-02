"""
SGAS authorization framework.
"""

import re

from twisted.python import log


# various actions
INSERT    = 'insert'
VIEW      = 'view'
QUERY     = 'query'

ACTIONS = [ INSERT, VIEW, QUERY ]

ALL_OPTION = 'all'

OPTIONS = {
    INSERT : [ ALL_OPTION ],
    VIEW   : [ ALL_OPTION ],
    QUERY  : [ ALL_OPTION ]
}

CONTEXTS = {
    INSERT : [ 'machine_name' ],
    VIEW   : [ 'view', 'viewgroup' ],
    QUERY  : [ 'machine_name', 'user_identity', 'vo_name' ]
}


# regular expression for matching authz lines
AUTHZ_RX = re.compile("""\s*"(.*)"\s*(.*)""")


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
    if request.getClientIP() in ('127.0.0.1', '::1') and 'x-ssl-subject' in request.received_headers:
        return request.received_headers.get('x-ssl-subject')

    # request wasn't secure or no certificate was presented
    return None



class AuthzRights:

    def __init__(self):
        self.options = []
        self.contexts = []


    def addOption(self, option):
        if not option in self.options:
            self.options.append(option)


    def addContext(self, context):
        if not context in self.contexts:
            self.contexts.append(context)



class Authorizer:

    def __init__(self, authz_file=None):
        self.authz_rights = {}  # subject -> [action : AuthzRights ]
        if authz_file is not None:
            authz_data = open(authz_file).read()
            self.parseAuthzData(authz_data)


    def parseAuthzData(self, authz_data):
        # parse authorization data given as input string

        authz_lines = authz_data.split('\n')

        for line in authz_lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            m = AUTHZ_RX.match(line)
            if not m:
                continue
            subject, action_segment = m.groups()

            user_authz_rights = self.authz_rights.setdefault(subject, {})
            for action_desc in action_segment.split(','):
                self._parseActions(action_desc.strip(), user_authz_rights)


    def _parseActions(self, action_desc, user_authz_rights):

        action_authzgroup = action_desc.split(':')

        if len(action_authzgroup) == 1:
            action = action_authzgroup[0]
            if not action in ACTIONS:
                log.msg('Invalid authz action: "%s", skipping entry.' % action_desc)
                return
            user_authz_rights.setdefault(action, AuthzRights())
            # some backwards compat
            if action == VIEW:
                # log.msg ...
                user_authz_rights[action].options.append(ALL_OPTION)

        elif len(action_authzgroup) == 2:
            action, authzgroup = action_authzgroup
            for authz in authzgroup.split(','):
                if '=' in authz: # context
                    ctx = {}
                    for ctx_group in authz.split('+'):
                        try:
                            ctx_key, ctx_values = ctx_group.split('=')
                            for ctx_val in ctx_values.split(';'):
                                ctx.setdefault(ctx_key, []).append(ctx_val)
                            action_rights = user_authz_rights.setdefault(action, AuthzRights())
                            action_rights.addContext(ctx)
                        except ValueError:
                            log.msg('Invalid authz context: %s, skipping entry.' % action_desc)
                else: # option
                    for option in authz.split('+'):
                        if not option in OPTIONS.get(action, []):
                            log.msg('Invalid authz option: %s, skipping entry.' % action_desc)
                            continue
                        action_rights = user_authz_rights.setdefault(action, AuthzRights())
                        action_rights.addOption(option)

        else:
            log.warning('Invalid authz group: "%s", skipping entry.' % action_desc)


    def hasRelevantRight(self, subject, action):
        """
        Checks if a subject has any rights regarding a certain action.

        This is usefull for checking very early in a process if a subject
        could be allowed to perform an action, before the details of the
        action is known.

        Most important usage of this is insertion, where it can be checked
        if a subject is in any way allowed to insert something before the
        actual records are parsed and the insert identity - machine name
        check is performed.

        Returns True if the subject has a relevant right, otherwise False.
        """
        if subject in self.authz_rights and action in self.authz_rights[subject]:
            return True
        else:
            return False


    def isAllowed(self, subject, action, context=None):
        """
        Checks if a subject is allowed to perform a certain action, within a given
        context.

        Returns True if the subject is allowed, otherwise False.
        """

        if action in [ VIEW, QUERY ]:
            assert context is not None, 'Actions view or query requires context'

        allowed = False

        try:
            user_authz_rights = self.authz_rights[subject]
            # subject is not found -> keyerror -> denied

            action_rights = user_authz_rights[action]
            # action is not found -> keyerror -> denied

            if ALL_OPTION in action_rights.options:
            # special all option is set for the subject -> granted
                allowed = True

            elif context is None and action == INSERT:
            # no context, means allowed for insert, otherwise not (do nothing)
                allowed = True

            else: # there is a context
                for ctx in action_rights.contexts:
                    ctx_allow = []
                    for cak, cav in ctx.items():
                        #print "CAK", cak, cav, context
                        found_match = False
                        for cik, civ in context:
                            if cak == cik:
                                found_match = True
                                if civ in cav:
                                    ctx_allow.append(True)
                                else:
                                    ctx_allow.append(False)

                        if not found_match:
                            ctx_allow.append(False)

                    allowed = all(ctx_allow or [False])
                    if allowed:
                        break

        except KeyError:
            pass

        #print "Authz check: Subject %s, Action %s, Context: %s. Access allowed: %s" % (subject, action, context, allowed)
        log.msg("Authz check: Subject %s, Action %s, Context: %s. Access allowed: %s" % \
                 (subject, action, context, allowed), system='sgas.Authorizer')

        return allowed


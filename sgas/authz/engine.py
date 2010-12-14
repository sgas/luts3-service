"""
SGAS authorization framework.
"""

import re

from twisted.python import log

from sgas.authz import rights, ctxinsertchecker, ctxsetchecker




# regular expression for matching authz lines
AUTHZ_RX = re.compile("""\s*"(.*)"\s*(.*)""")



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



class AuthorizationEngine:

    def __init__(self, insert_check_depth, authz_file=None):
        self.authz_rights = {}  # subject -> [action : AuthzRights ]
        if authz_file is not None:
            authz_data = open(authz_file).read()
            self.parseAuthzData(authz_data)

        self.context_checkers = {
            rights.ACTION_INSERT : ctxinsertchecker.InsertChecker(insert_check_depth),
            rights.ACTION_VIEW   : ctxsetchecker.AnySetChecker,
            rights.ACTION_QUERY  : ctxsetchecker.AllSetChecker
        }


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
            if not action in rights.ACTIONS:
                log.msg('Invalid authz action: "%s", skipping entry.' % action_desc, system='sgas.Authorizer')
                return
            user_authz_rights.setdefault(action, AuthzRights())
            # some backwards compat
            if action == rights.ACTION_VIEW:
                log.msg("Expanding 'view' stanza to 'view:all'. Please change to 'view:all'. This behaviour might change in the future", system='sgas.Authorizer')
                user_authz_rights[action].options.append(rights.OPTION_ALL)

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
                            log.msg('Invalid authz context: %s, skipping entry.' % action_desc, system='sgas.Authorizer')
                else: # option
                    for option in authz.split('+'):
                        if not option in rights.OPTIONS.get(action, []):
                            log.msg('Invalid authz option: %s, skipping entry.' % action_desc, system='sgas.Authorizer')
                            continue
                        action_rights = user_authz_rights.setdefault(action, AuthzRights())
                        action_rights.addOption(option)

        else:
            log.warning('Invalid authz group: "%s", skipping entry.' % action_desc, system='sgas.Authorizer')


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


    def isAllowed(self, subject, action, context):
        """
        Checks if a subject is allowed to perform a certain action, within a given
        context.

        Returns True if the subject is allowed, otherwise False.
        """

        allowed = False

        try:
            user_authz_rights = self.authz_rights[subject]
            # subject is not found -> keyerror -> denied

            user_action_rights = user_authz_rights[action]
            # action is not found -> keyerror -> denied

            if rights.OPTION_ALL in user_action_rights.options:
            # special all option is set for the subject -> granted
                allowed = True

            # perform context check
            ctx_checker = self.context_checkers[action]
            if ctx_checker.contextCheck(subject, user_action_rights.contexts, context):
                allowed = True

        except KeyError:
            pass

        log.msg("Authz check: Subject %s, Action %s, Context: %s. Access allowed: %s" % \
                 (subject, action, context, allowed), system='sgas.Authorizer')

        return allowed


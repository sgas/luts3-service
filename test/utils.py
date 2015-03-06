"""
Various testing utilities.
"""

from twisted.python import log


class FakeAuthorizer:
    # silly authorizer for testing setups without authz part

    def hasRelevantRight(self, subject, action):
        return True

    def isAllowed(self, subject, action, context=None):
        log.msg('Authz check (fake): %s, %s, %s' % (subject, action, context))
        return True
    
    def initAuthzFile(self, authz_file=None):
        pass
    
    def addChecker(self, action, checker):
        pass


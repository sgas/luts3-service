"""
Various testing utilities.
"""


class FakeAuthorizer:
    # silly authorizer for testing setups without authz part

    def hasRelevantRight(self, subject, action):
        return True

    def isAllowed(self, subject, action, context=None):
        return True


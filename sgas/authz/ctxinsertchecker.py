"""
Usage Records insertion checker.

Provides functionality for checking if a host should be able to insert a given
usage record.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009, 2010)
"""



from sgas.authz import rights



class InsertChecker:

    CONTEXT_KEY = None

    def __init__(self, check_depth):

        self.check_depth = check_depth


    def contextCheck(self, subject_identity, subject_rights, action_context):
        """
        Given a (x509) subject identity, subject rights and a context with for the
        insertion, this function decides if the subject is allowed to perform insertion
        for the given context.

        This is done both with specific checking of specified context, and by checking
        "similarity" between the subject identity host name and the action context.
        """
        if action_context is None:
            return True # compat mode

        subject_fqdn = extractFQDNfromX509Identity(subject_identity)
        insert_context = [ ctx_value for ctx_key, ctx_value in action_context if ctx_key == self.CONTEXT_KEY ]

        # insert context explicitely allowed
        explicit_allowed_contexts = []
        for sr in subject_rights:
            explicit_allowed_contexts += sr.get(self.CONTEXT_KEY, [])

        # subject name parts for depth checking
        id_parts = [ p for p in subject_fqdn.split('.') if p != '' ]
        cd = min(self.check_depth, len(id_parts))

        # go through all requested machine names and check if insert is allowed
        allowed = []
        for ic in insert_context:
            if ic in explicit_allowed_contexts:
                allowed.append(True)
                continue

            # check if x509 identity is close enough to machine name to allow insertion
            ic_parts = [ p for p in ic.split('.') if p != '' ]
            if len(ic_parts) < cd:
                allowed.append(False)
                continue

            for d in range( - cd, 0):
                if ic_parts[d] != id_parts[d]:
                    allowed.append(False)
                    break
            else:
                # for loop terminated without breaking, check depth ok
                allowed.append(True)

        return all(allowed)


def extractFQDNfromX509Identity(identity):
    """
    Givens strings like:

    "/O=Grid/O=NorduGrid/CN=benedict.grid.aau.dk"
    "/O=Grid/O=NorduGrid/CN=host/fyrkat.grid.aau.dk"

    this function returns the FQDN of the identity.
    """
    if identity is None:
        return '.' # this is technically a hostname

    tokens = identity.split('/')

    if len(tokens) == 1:
        return identity # not an x509 identity

    if tokens[-2] == 'CN=host':
        fqdn = tokens[-1]
    elif tokens[-1].startswith('CN='):
        fqdn = tokens[-1].split('=',2)[1]
    else:
        raise ValueError('Could not extract FQDN from X509 identity (%s)' % identity)

    if not '.' in fqdn:
        raise ValueError('Extracted FQDN is not an FQDN (%s)' % fqdn)

    return fqdn


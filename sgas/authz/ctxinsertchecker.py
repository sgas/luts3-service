"""
Usage Records insertion checker.

Provides functionality for checking if a host should be able to insert a given
usage record.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009, 2010)
"""



from sgas.authz import rights



class InsertChecker:

    def __init__(self, check_depth):

        self.check_depth = check_depth


    def contextCheck(self, subject_identity, subject_rights, action_context):
        """
        Given a (x509) subject identity, subject rights and a context with machine
        names, this function decides if the subject is allowed to perform insertion
        on the machine names specified in the action context.

        This is done both with specific checking of allowed host names, and by checking
        "similarity" between the subject identity host name and the machine names in
        the action context.
        """
        if action_context is None:
            return True # compat mode

        subject_fqdn = extractFQDNfromX509Identity(subject_identity)
        machine_names = [ ctx_value for ctx_key, ctx_value in action_context if ctx_key == rights.CTX_MACHINE_NAME ]

        # machine names explicitely allowed
        sr_machine_names = []
        for sr in subject_rights:
            sr_machine_names += sr.get(rights.CTX_MACHINE_NAME, [])

        # subject name parts for depth checking
        id_parts = [ p for p in subject_fqdn.split('.') if p != '' ]
        cd = min(self.check_depth, len(id_parts))

        # go through all requested machine names and check if insert is allowed
        allowed = []
        for mn in machine_names:
            if mn in sr_machine_names:
                allowed.append(True)
                continue

            # check if x509 identity is close enough to machine name to allow insertion
            mn_parts = [ p for p in mn.split('.') if p != '' ]
            if len(mn_parts) < cd:
                allowed.append(False)
                continue

            for d in range( - cd, 0):
                if mn_parts[d] != id_parts[d]:
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


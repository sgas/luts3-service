"""
Usage Records insertion checker.

Simple abstraction over checking if a host should be able to insert a given
usage record.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009, 2010)
"""


from sgas.server import authz


class InsertionChecker:

    def __init__(self, check_depth, authorizer):

        self.check_depth = check_depth
        self.authorizer = authorizer


    def isInsertAllowed(self, x509_identity, ur_machine_name):
        """
        Given an x509 identity and a UR machine name, this function will decide
        if the host is allowed to insert the given record.

        Note that the two does not have to be identical, but only belong to the
        certain domin of a certain depth, e.g., ce01.titan.uio.no should be
        allowed to register records as titan.uio.no.

        Additionally it is possible for a subject to be allowed to insert on behalf
        of all machines or a specific set of machines.
        """
        fqdn = extractFQDNfromX509Identity(x509_identity)

        if self.authorizer.isAllowed(x509_identity, authz.INSERT, context={'machine_name': ur_machine_name}):
            return True # authorizer says ok

        # check if x509 identity is close enough to machine name to allow insertion on default
        # insert premise

        # depth checking
        id_parts = [ e for e in fqdn.split('.') if e != '' ]
        mn_parts = [ e for e in ur_machine_name.split('.') if e != '' ]

        for d in range( - self.check_depth, 0):
            if mn_parts[d] != id_parts[d]:
                return False

        # for loop terminated, check depth ok
        return True



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

    if tokens[-2] == 'CN=host':
        fqdn = tokens[-1]
    elif tokens[-1].startswith('CN='):
        fqdn = tokens[-1].split('=',2)[1]
    else:
        raise ValueError('Could not extract FQDN from X509 identity (%s)' % identity)

    if not '.' in fqdn:
        raise ValueError('Extracted FQDN is not an FQDN (%s)' % fqdn)

    return fqdn


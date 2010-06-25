"""
Utility functions for checking if machine name fields and insert identity
match (within reason).

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log

from sgas.database import error


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



def checkMatch(machine_name, fqdn, check_depth=2):
    """
    Checks that a given machine_name (from a usage record) matches with
    an FQDN (extracted from the insert identity. Note that the two does
    not have to be identical, but only belong to the certain domin of a
    certain depth, e.g., ce01.titan.uio.no should be allowed to register
    records as titan.uio.no.
    """

    MSG = 'Machine name (%s) does not match FQDN of identity (%s) to sufficient degree'

    mn_parts = [ e for e in machine_name.split('.') if e != '' ]
    id_parts = [ e for e in fqdn.split('.') if e != '' ]

    for d in range(-check_depth, 0):
        if mn_parts[d] != id_parts[d]:
            raise error.SecurityError(MSG % (machine_name, fqdn))


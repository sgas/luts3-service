"""
HTTP server resource utilities.
"""

import re
import socket

from twisted.python import log

from sgas.server.util import has_headers, get_headers


LOOPBACK_ADDRESSES = ('127.0.0.1', '::1')

X_SSL_SUBJECT   = "x-ssl-subject"
X_FORWARDED_FOR = "x-forwarded-for"

IP_REGEX = "^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
ip_matcher = re.compile(IP_REGEX)



def getSubject(request):
    """
    Utility method for extracting the subject name from a twisted.web.http.Request
    """
    # identity forwarded by reverse proxy
    if request.getClientIP() in LOOPBACK_ADDRESSES and has_headers(request, X_SSL_SUBJECT):
        return get_headers(request, X_SSL_SUBJECT)

    # request wasn't secure or no certificate was presented
    return None


def getHostname(request):
    """
    Utility method for getting hostname of client.
    """
    if request.getClientIP() in LOOPBACK_ADDRESSES and has_headers(request, X_FORWARDED_FOR):
        # nginx typically returns ip addresses
        addr = get_headers(request, X_FORWARDED_FOR)
        if isIPAddress(addr):
            # we really shouldn't do such blocking calls in twisted,
            # but the twisted dns interface is rather terrible and
            # odd things happen when using it
            # Set timeout to 1 second to limit the possible damage
            try:
                socket.setdefaulttimeout(1)
                info = socket.gethostbyaddr(addr)
                return info[0]
            except socket.error as msg:
                log.msg("Error performing reverse lookup: %s" % msg)
                return addr
        else:
            addr

    else:
        hostname = request.getClient()
        if hostname is None:
            hostname = request.getClientIP()
        return hostname


def getCN(dn):
    """
    Given DN strings like:

    (old format)

    "/O=Grid/O=NorduGrid/CN=benedict.grid.aau.dk"
    "/O=Grid/O=NorduGrid/CN=host/fyrkat.grid.aau.dk"

     or (new format)

    "CN=alice-mon.ndgf.org,OU=Nordic DataGrid Facility,O=Nordunet a/s,L=Kastrup,ST=Hovedstaden,C=DK,DC=tcs,DC=terena,DC=org"

    this function returns the CN.
    """
    if dn is None:
        return '.' # this is technically a hostname

    if dn[0] == '/':
        # Seems to be the old style '/'-delimited format

        tokens = dn.split('/')

        if len(tokens) == 1:
            return dn # not an x509 identity


        if tokens[-2] == 'CN=host':
            cn = tokens[-1]
        elif tokens[-1].startswith('CN='):
            cn = tokens[-1].split('=',2)[1]
        else:
            raise ValueError('Could not extract CN from X509 identity (%s)' % identity)
    else:
        # Assume newer ','-delimited format

        tokens = dn.split(',')

        cn = None
        for t in tokens:
            if t.startswith('CN='):
                cn = t.split('=',2)[1]

        if not cn:
            raise ValueError('Could not extract CN from X509 identity (%s)' % identity)

    return cn



def isIPAddress(addr):

    if ip_matcher.match(addr):
        return True
    else:
        return False


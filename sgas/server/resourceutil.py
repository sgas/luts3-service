"""
HTTP server resource utilities.
"""

import re
import socket

from twisted.python import log


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
    if request.getClientIP() in LOOPBACK_ADDRESSES and X_SSL_SUBJECT in request.received_headers:
        return request.received_headers.get(X_SSL_SUBJECT)

    # request wasn't secure or no certificate was presented
    return None


def getHostname(request):
    """
    Utility method for getting hostname of client.
    """
    if request.getClientIP() in LOOPBACK_ADDRESSES and X_FORWARDED_FOR in request.received_headers:
        # nginx typically returns ip addresses
        addr = request.received_headers.get(X_FORWARDED_FOR)
        if isIPAddress(addr):
            # we really shouldn't do such blocking calls in twisted,
            # but the twisted dns interface is rather terrible and
            # odd things happen when using it
            # Set timeout to 1 second to limit the possible damage
            try:
                socket.setdefaulttimeout(1)
                info = socket.gethostbyaddr(addr)
                return info[0]
            except socket.error, msg:
                log.msg("Error performing reverse lookup: %s" % msg)
                return addr
        else:
            addr

    else:
        hostname = request.getClient()
        if hostname is None:
            hostname = request.getClientIP()
        return hostname


def isIPAddress(addr):

    if ip_matcher.match(addr):
        return True
    else:
        return False


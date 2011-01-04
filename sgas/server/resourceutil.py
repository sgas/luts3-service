"""
HTTP server resource utilities.
"""

LOOPBACK_ADDRESSES = ('127.0.0.1', '::1')

X_SSL_SUBJECT   = "x-ssl-subject"
X_FORWARDED_FOR = "x-forwarded-for"


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
    if request.getClientIP() in LOOPBACK_ADDRESSES and X_SSL_SUBJECT in request.received_headers:
        return request.received_headers.get(X_SSL_SUBJECT)

    # request wasn't secure or no certificate was presented
    return None


def getHostname(request):
    """
    Utility method for getting hostname of client.
    """
    if request.getClientIP() in LOOPBACK_ADDRESSES and X_FORWARDED_FOR in request.received_headers:
        return request.received_headers.get(X_FORWARDED_FOR)

    else:
        hostname = request.getClient()
        if hostname is None:
            hostname = request.getClientIP()
        return hostname


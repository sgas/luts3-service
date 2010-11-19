"""
HTTP server resource utilities.
"""



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
    if request.getClientIP() in ('127.0.0.1', '::1') and 'x-ssl-subject' in request.received_headers:
        return request.received_headers.get('x-ssl-subject')

    # request wasn't secure or no certificate was presented
    return None


def getHostname(request):
    """
    Utility method for getting hostname of client.
    """
    if request.getClientIP() in ('127.0.0.1', '::1') and 'X-Forwarded-For' in request.received_headers:
        return request.received_headers.get('x-forwarded-for')

    else:
        hostname = request.getClient()
        if hostname is None:
            hostname = request.getClientIP()
        return hostname


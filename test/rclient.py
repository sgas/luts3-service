"""
This module contains a copy of the http client code used in the
arc-ur-registrant (and similar) in order to test the server as realistic as
possible.
"""

from twisted.internet import reactor
from twisted.web import client


def httpRequest(url, method='GET', payload=None, ctxFactory=None):
    # probably need a header options as well
    """
    Peform a http request.
    """
    # copied from twisted.web.client in order to get access to the
    # factory (which contains response codes, headers, etc)

    scheme, host, port, path = client._parse(url)
    factory = client.HTTPClientFactory(url, method=method, postdata=payload)
    factory.noisy = False # stop spewing about factory start/stop
    # fix missing port in header (bug in twisted.web.client)
    if port:
        factory.headers['host'] = host + ':' + str(port)

    if scheme == 'https':
        reactor.connectSSL(host, port, factory, ctxFactory)
    else:
        reactor.connectTCP(host, port, factory)

    #factory.deferred.addBoth(f, factory)
    return factory.deferred, factory


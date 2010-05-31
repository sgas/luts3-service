"""
Twisted couch db client library.

Created as I did not like the abstractions in paisley.

Author: Henrik Thostrup Jensen <thostrup@gmail.com>
"""

import json
import urllib


from twisted.python import log
from twisted.internet import reactor, defer
from twisted.web import client



DEFAULT_PORT = 5984

REVISION_KEY = '_rev'
ID_KEY = '_id'

ALL_DOCS  = '_all_docs'
BULK_DOCS = '_bulk_docs'
TEMP_VIEW = '_temp_view'
VIEW      = '_view'
DESIGN    = '_design'


# errors

class DatabaseUnavailableError(Exception):
    """
    Raised when a contacted database is not available.
    """


class NoSuchDocumentError(Exception):
    """
    Raised when trying to access a document which does not exist.
    """


class DatabaseAlreadyExistsError(Exception):
    """
    Raised when trying to create a database that already exists.
    """


class DocumentAlreadyExistsError(Exception):
    """
    Raised when trying to create a document, but a document with the same
    name already exists.
    """


class ViewCreationError(Exception):
    """
    Raised when a view could not be created.
    """


class InvalidViewError(Exception):
    """
    Raised when attempting to access a non-existing view.
    """


class UnhandledResponseError(Exception):
    """
    Raised when the client library does not know how to deal with a specific response.
    """



# utility functions


def httpRequest(url, method, data=None):
    # somewhat copied from twisted.web.client in order to get access to the
    # factory (which contains response codes, headers, etc)

    scheme, host, port, path = client._parse(url)
    assert scheme == 'http', 'Only HTTP protocol is supported'

    headers = {"Accept": "application/json"}

    factory = client.HTTPClientFactory(url, method=method, postdata=data, headers=headers)
    factory.noisy = False # stop spewing about factory start/stop

    # fix missing port in header (bug in twisted.web.client)
    if port: factory.headers['host'] = host + ':' + str(port)

    reactor.connectTCP(host, port, factory)

    return factory.deferred, factory



def couchRequest(url, method, data=None):
    """
    Wrapper for making http request to couchdb, which only errs out on real
    errors (and not every non-200 response).
    """
    def handleResponse(result, factory):
        if hasattr(factory, 'status'):
            return result
        raise DatabaseUnavailableError(result.getErrorMessage())

    d, f = httpRequest(url, method, data)
    d.addBoth(handleResponse, f)
    return d, f



# class definitions

class CouchDB:

    def __init__(self, url):
        if not url.endswith('/'):
            url += '/'
        self.url = url


    def createDatabase(self, db_name):

        def handleResponse(result, factory):
            if factory.status == '201':
                return Database(self.url + db_name)
            log.msg("Error creating CouchDB database (%s)" % result, system='sgas.CouchDB')
            if factory.status == '409':
                e = DatabaseAlreadyExistsError('Database with name %s already exists' % db_name)
                return defer.fail(e)
            e = UnhandledResponseError('Unhandled response. Response code: %s' % factory.status)
            return defer.fail(e)

        d, f = couchRequest(self.url + db_name, method='PUT')
        d.addCallback(handleResponse, f)
        return d


    def openDatabase(self, db_name):
        return Database(self.url + db_name)


    def deleteDatabase(self, db_name):

        def handleResponse(result, factory):
            if factory.status == '200':
                return
            e = UnhandledResponseError('Unhandled response. Response code: %s' % factory.status)
            return defer.fail(e)

        d, f = couchRequest(self.url + db_name, method='DELETE')
        d.addCallback(handleResponse, f)
        return d



class Database:

    def __init__(self, url):
        if not url.endswith('/'):
            url += '/'
        self.url = url

    # db commands

    def info(self):

        def handleResponse(result, factory):
            if factory.status == '200': # all'correct
                return json.loads(result)
            else:
                e = UnhandledResponseError('Unhandled response. Response code: %s' % factory.status)
                return defer.fail(e)

        d, f = couchRequest(self.url, 'GET')
        d.addCallback(handleResponse, f)
        return d


    def listDocuments(self):

        def handleResponse(result, factory):
            if factory.status == '200':
                return json.loads(result)
            defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        d, f = couchRequest(self.url + ALL_DOCS, method='GET')
        d.addCallback(handleResponse, f)
        return d

    # single document commands

    def retrieveDocument(self, doc_id):

        def handleResponse(result, factory):
            if factory.status == '200':
                return json.loads(result)
            elif factory.status == '404':
                return defer.fail(NoSuchDocumentError('No such document (%s) in database' % doc_id))
            return defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        d, f = couchRequest(self.url + doc_id, method='GET')
        d.addBoth(handleResponse, f)
        return d


    def createDocument(self, doc, doc_id=None):

        def handleResponse(result, factory):
            if factory.status == '201':
                return json.loads(result)
            log.msg("Error creating document (%s)" % result, system='sgas.Database')
            if factory.status == '409':
                return defer.fail(DatabaseAlreadyExistsError('Document with name %s already exists' % doc_id))
            return defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        payload = json.dumps(doc)
        if doc_id is None and ID_KEY not in doc:
            d, f = couchRequest(self.url, method='POST', data=payload)
        else:
            if not doc_id:
                doc_id = str(doc[ID_KEY])
            d, f = couchRequest(self.url + doc_id, method='PUT', data=payload)

        d.addCallback(handleResponse, f)
        return d


    def deleteDocument(self, doc):
        def handleResponse(result, factory):
            if factory.status == '200':
                return json.loads(result)
            return defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        # deal with doc being a proper document or just a string
        if type(doc) is dict:
            doc_id = str(doc[ID_KEY]) + '?' + urllib.urlencode({'rev':doc[REVISION_KEY]})
        else:
            doc_id = doc

        d, f = couchRequest(self.url + doc_id, method='DELETE')
        d.addCallback(handleResponse, f)
        return d

    # document bulk commands

    # this one needs couchdb 0.9+ to work
    # doc_ids should be an array of wanted keys
    def retrieveDocuments(self, doc_ids=None, startkey=None, endkey=None):

        def handleResponse(result, factory):
            if factory.status == '200':
                return json.loads(result)
            log.msg("Error fetching documents", system='sgas.Database')
            return defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        request_url = self.url + ALL_DOCS + '?include_docs=true'
        payload = None

        if doc_ids is not None:
            assert startkey is None, 'Cannot specify doc_ids and startkey'
            assert endkey   is None, 'Cannot specify doc_ids and endkey'
            payload = json.dumps({'keys': doc_ids})

        if startkey is not None:
            assert doc_ids is None, 'Cannot use doc_ids with startkey'
            request_url += '&startkey="%s"' % urllib.quote(startkey)

        if endkey is not None:
            assert doc_ids is None, 'Cannot use doc_ids with endkey'
            request_url += '&endkey="%s"' % urllib.quote(endkey)

        d, f = couchRequest(request_url, method='GET', data=payload)
        d.addCallback(handleResponse, f)
        return d


    def insertDocuments(self, docs):
        def handleResponse(result, factory):
            if factory.status == '201':
                return json.loads(result)
            if factory.status == '412':
                return defer.fail(DocumentAlreadyExistsError('Got conflict from database'))
            return defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        payload = json.dumps({'docs': docs })
        d, f = couchRequest(self.url + BULK_DOCS, method='POST', data=payload)
        d.addCallback(handleResponse, f)
        return d

    # view commands

    def temporaryView(self, view_definition):
        def handleResponse(result, factory):
            if factory.status == 200:
                return json.loads(result)
            return defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        payload = """{ "map" : "%s" }""" % view_definition
        d, f = couchRequest(self.url + TEMP_VIEW, method='POST', data=payload)
        d.addCallback(handleResponse, f)
        return d


    def createView(self, view_name, view_definition):
        def handleResponse(result, factory):
            if factory.status == '201':
                return json.loads(result)
            elif factory.status == '500':
                return defer.fail(ViewCreationError(result))
            defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        d, f = couchRequest(self.url + DESIGN + '/' + view_name, method='PUT', data=view_definition)
        d.addCallback(handleResponse, f)
        return d

    # need a delete view / get view design


    def queryView(self, design_name, view_name, group=True, startkey=None, endkey=None):

        def handleResponse(result, factory):
            if factory.status == '200':
                return json.loads(result)
            elif factory.status == '404':
                return defer.fail(InvalidViewError('View %s/%s does not exist' % (design_name, view_name)))
            defer.fail(UnhandledResponseError('Unhandled response. Response code: %s' % factory.status))

        url = self.url + DESIGN + '/' + design_name + '/' + VIEW + '/' + view_name

        params = []
        if group:
            params.append('group=true')
        if startkey is not None:
            params.append('startkey=%s' % urllib.quote(startkey))
        if endkey is not None:
            params.append('endkey=%s' % urllib.quote(endkey))
        if params:
            url += '?' + '&'.join(params)

        d, f = couchRequest(url, method='GET')
        d.addCallback(handleResponse, f)
        return d


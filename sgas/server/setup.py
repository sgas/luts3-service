"""
Server-setup logic
"""
import os.path

from twisted.python import log
from twisted.application import internet, service
from twisted.web import resource, server

from sgas import __version__
from sgas.server import config, ssl, authz, hostcheck, topresource, insertresource, viewresource, queryresource
from sgas.database.postgresql import database as pgdatabase
from sgas.viewengine import viewdefinition


# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

TCP_PORT = 6180
SSL_PORT = 6143



class ConfigurationError(Exception):
    """
    Raised when the server is configured wrong.
    """



def createSite(db, authorizer, views):

    rr = insertresource.InsertResource(db, authorizer)
    vr = viewresource.ViewTopResource(db, authorizer, views)
    qr = queryresource.QueryResource(db, authorizer)

    tr = topresource.TopResource(authorizer)
    tr.registerService(rr, 'ur', (('Registration', 'ur'),) )
    tr.registerService(vr, 'view', (('View', 'view'),))
    tr.registerService(qr, 'query', (('Query', 'query'),))

    root = resource.Resource()
    root.putChild('sgas', tr)

    site = server.Site(root)
    return site



def createSGASServer(config_file=DEFAULT_CONFIG_FILE, use_ssl=None, port=None):

    cfg = config.readConfig(config_file)

    # first figure out if SGAS should run behind a reverse proxy

    cfg_proxy_value = cfg.get(config.SERVER_BLOCK, config.REVERSE_PROXY).lower()
    if cfg_proxy_value == 'true':
        reverse_proxy = True
    elif cfg_proxy_value == 'false':
        reverse_proxy = False
    else:
        raise ConfigurationError('Invalid value for reverse_proxy in configuration.')

    if reverse_proxy:
        use_ssl = False

    # if ssl usage hasn't been defined, we default to true for backward compatability
    if use_ssl is None:
        use_ssl = True

    # ssl sanity checks
    if use_ssl:
        hostkey  = cfg.get(config.SERVER_BLOCK, config.HOSTKEY)
        hostcert = cfg.get(config.SERVER_BLOCK, config.HOSTCERT)
        certdir  = cfg.get(config.SERVER_BLOCK, config.CERTDIR)
        if not (os.path.exists(hostkey)  and os.path.isfile(hostkey)):
            raise ConfigurationError('Configured host key does not exist')
        if not (os.path.exists(hostcert) and os.path.isfile(hostcert)):
            raise ConfigurationError('Configured host cert does not exist')
        if not (os.path.exists(certdir)  and os.path.isdir(certdir)):
            raise ConfigurationError('Configured certificate directory does not exist')

    # setup server

    db_url = cfg.get(config.SERVER_BLOCK, config.DB)
    try:
        check_depth = int(cfg.get(config.SERVER_BLOCK, config.HOSTNAME_CHECK_DEPTH))
    except ValueError: # in case casting goes wrong
        raise ConfigurationError('Configured check depth is invalid')

    # check for whitelist and fail if it is there
    # a bit harsh, but the config needs to be updated for the system to work
    cfg_whitelist = cfg.get(config.SERVER_BLOCK, config.HOSTNAME_CHECK_WHITELIST)
    if cfg_whitelist:
        raise ConfigurationError('Whitelist no longer supported, use "insert:all" in sgas.authz')

    # authz
    authorizer = authz.Authorizer(cfg.get(config.SERVER_BLOCK, config.AUTHZ_FILE))
    checker = hostcheck.InsertionChecker(check_depth, authorizer)

    # database
    if db_url.startswith('http'):
        raise ConfigurationError('CouchDB no longer supported. Please upgrade to PostgreSQL')

    db = pgdatabase.PostgreSQLDatabase(db_url, checker)

    # http site
    views = viewdefinition.buildViewList(cfg)
    site = createSite(db, authorizer, views)

    # application
    application = service.Application("sgas")

    db.setServiceParent(application)

    if use_ssl:
        hostkey  = cfg.get(config.SERVER_BLOCK, config.HOSTKEY)
        hostcert = cfg.get(config.SERVER_BLOCK, config.HOSTCERT)
        certdir  = cfg.get(config.SERVER_BLOCK, config.CERTDIR)
        cf = ssl.ContextFactory(key_path=hostkey, cert_path=hostcert, ca_dir=certdir)
        internet.SSLServer(port or SSL_PORT, site, cf).setServiceParent(application)
    elif reverse_proxy:
        internet.TCPServer(port or TCP_PORT, site, interface='localhost').setServiceParent(application)
    else:
        internet.TCPServer(port or TCP_PORT, site).setServiceParent(application)

    log.msg('SGAS %s twistd application created, starting server.' % __version__)

    return application


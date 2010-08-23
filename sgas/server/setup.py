"""
Server-setup logic
"""
import os.path

from twisted.application import internet, service
from twisted.web import resource, server

from sgas.server import config, ssl, authz, hostcheck, topresource, insertresource, viewresource
from sgas.viewengine import viewdefinition


# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

TCP_PORT = 6180
SSL_PORT = 6143



class ConfigurationError(Exception):
    """
    Raised when the server is configured wrong.
    """


def buildViewList(cfg):

    views = []

    for block in cfg.sections():
        if block.startswith(config.VIEW_PREFIX):
            view_name = block.split(':',1)[-1]
            view_args = dict(cfg.items(block))
            view = viewdefinition.createViewDefinition(view_name, view_args)
            views.append(view)

    return views



def createSite(db, authorizer, views):

    rr = insertresource.InsertResource(db, authorizer)
    vr = viewresource.ViewTopResource(db, authorizer, views)

    tr = topresource.TopResource(authorizer)
    tr.registerService(rr, 'ur', (('Registration', 'ur'),) )
    tr.registerService(vr, 'view', (('View', 'view'),))

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
        raise ConfigurationError('Invalid value for reverseproxy in configuration.')

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
    # read whitelist, but filter out '' values
    cfg_whitelist = cfg.get(config.SERVER_BLOCK, config.HOSTNAME_CHECK_WHITELIST)
    check_whitelist = [ x.strip() for x in cfg_whitelist.split(',') if x.strip() ]

    checker = hostcheck.InsertionChecker(check_depth, whitelist=check_whitelist)

    # database
    if db_url.startswith('http'):
        from sgas.database.couchdb import database
        db = database.CouchDBDatabase(db_url, checker)
    else:
        from sgas.database.postgresql import database
        db = database.PostgreSQLDatabase(db_url, checker)

    # http site
    authorizer = authz.Authorizer(cfg.get(config.SERVER_BLOCK, config.AUTHZ_FILE))
    views = buildViewList(cfg)
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

    return application


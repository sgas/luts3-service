"""
Server-setup logic
"""
import os.path

from twisted.application import internet, service
from twisted.web import resource, server

from sgas.server import config, ssl, authz, topresource, insertresource, viewresource
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



def createSite(db, authorizer, views, web_files_path):

    rr = insertresource.InsertResource(db, authorizer)
    vr = viewresource.ViewTopResource(db, authorizer, views)

    tr = topresource.TopResource(authorizer)
    tr.registerService(rr, 'ur', (('Registration', 'ur'),) )
    tr.registerService(vr, 'view', (('View', 'view'),))

    root = resource.Resource()
    root.putChild('sgas', tr)

    site = server.Site(root)
    return site



def createSGASServer(config_file=DEFAULT_CONFIG_FILE, use_ssl=True, port=None):

    cfg = config.readConfig(config_file)

    # sanity checks
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
    except ValueError:
        check_depth = config.DEFAULT_HOSTNAME_CHECK_DEPTH

    if db_url.startswith('http'):
        from sgas.database.couchdb import database
        db = database.CouchDBDatabase(db_url, check_depth)
    else:
        from sgas.database.postgresql import database
        db = database.PostgreSQLDatabase(db_url, check_depth)

    views = buildViewList(cfg)
    authorizer = authz.Authorizer(cfg.get(config.SERVER_BLOCK, config.AUTHZ_FILE))
    web_files_path = cfg.get(config.SERVER_BLOCK, config.WEB_FILES)

    site = createSite(db, authorizer, views, web_files_path)

    # setup application
    application = service.Application("sgas")

    db.setServiceParent(application)

    if use_ssl:
        hostkey  = cfg.get(config.SERVER_BLOCK, config.HOSTKEY)
        hostcert = cfg.get(config.SERVER_BLOCK, config.HOSTCERT)
        certdir  = cfg.get(config.SERVER_BLOCK, config.CERTDIR)
        cf = ssl.ContextFactory(key_path=hostkey, cert_path=hostcert, ca_dir=certdir)
        internet.SSLServer(port or SSL_PORT, site, cf).setServiceParent(application)
    else:
        internet.TCPServer(port or TCP_PORT, site).setServiceParent(application)

    return application


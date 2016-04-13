"""
Server-setup logic
"""
import time

from twisted.application import internet, service
from twisted.web import resource, server

from sgas import __version__
from sgas.authz import engine
from sgas.server import config, messages, topresource, loadclass
from sgas.database.postgresql import database as pgdatabase



# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

DEFAULT_PORT = 6180



class ConfigurationError(Exception):
    """
    Raised when the server is configured wrong.
    """

def createSite(cfg, log, db, authorizer):
    tr = topresource.TopResource(authorizer)
    
    for pluginClass in loadclass.loadClassType(cfg,log,'site'):
        # instanciate        
        obj = pluginClass(cfg, db, authorizer)
        
        # register
        tr.registerService(obj, obj.PLUGIN_ID, ((obj.PLUGIN_NAME,obj.PLUGIN_ID),))
                           
    root = resource.Resource()
    root.putChild('sgas', tr)

    site = server.Site(root)
    site.log = lambda *args : None
    return site

def loadServices(cfg,log,db):
    for pluginClass in loadclass.loadClassType(cfg,log,'service'):
        # instanciate        
        obj = pluginClass(cfg, db)
        
        # Attach services to DB (TODO: make own master service)
        obj.setServiceParent(db)

def createSGASServer(config_file=DEFAULT_CONFIG_FILE, no_authz=False, port=None):

    log = messages.Messages()

    cfg = config.readConfig(config_file)

    # check for whitelist and fail if it is there
    # a bit harsh, but the config needs to be updated for the system to work
    if config.HOSTNAME_CHECK_WHITELIST in cfg.options(config.SERVER_BLOCK):
        raise ConfigurationError('Whitelist no longer supported, use "insert:all" in sgas.authz')

    # some friendly messages from your service configuration
    if config.HOSTKEY in cfg.options(config.SERVER_BLOCK):
        log.msg('Option "hostkey" can be removed from config file (no longer used)', system='sgas.Setup')
    if config.HOSTCERT in cfg.options(config.SERVER_BLOCK):
        log.msg('Option "hostcert" can be removed from config file (no longer used)', system='sgas.Setup')
    if config.CERTDIR in cfg.options(config.SERVER_BLOCK):
        log.msg('Option "certdir" can be removed from config file (no longer used)', system='sgas.Setup')
    if config.REVERSE_PROXY in cfg.options(config.SERVER_BLOCK):
        log.msg('Option "reverse_proxy" can be removed from config file (no longer used)', system='sgas.Setup')

    # check depth
    try:
        check_depth = cfg.getint(config.SERVER_BLOCK, config.HOSTNAME_CHECK_DEPTH)
    except ValueError: # in case casting goes wrong
        raise ConfigurationError('Configured check depth is invalid')

    # authz
    if no_authz:
        from test import utils
        authorizer = utils.FakeAuthorizer()
    else:
        authorizer = engine.AuthorizationEngine(check_depth, cfg.get(config.SERVER_BLOCK, config.AUTHZ_FILE))

    # database
    db_url = cfg.get(config.SERVER_BLOCK, config.DB)
    if db_url.startswith('http'):
        raise ConfigurationError('CouchDB no longer supported. Please upgrade to PostgreSQL')
    db = pgdatabase.PostgreSQLDatabase(db_url)

    # hs.setServiceParent(db)

    # http site
    site = createSite(cfg, log, db, authorizer)
    
    # load generic services
    loadServices(cfg,log,db)
    
    # read auth file.
    authorizer.initAuthzFile()

    # application
    application = service.Application("sgas")

    log.setServiceParent(application)
    db.setServiceParent(application)

    internet.TCPServer(port or DEFAULT_PORT, site, interface='localhost').setServiceParent(application)

    log.msg('SGAS %s twistd application created, starting server.' % __version__, system='sgas.Setup')

    return application


"""
Server-setup logic
"""
import time

from twisted.application import internet, service
from twisted.web import resource, server

from sgas import __version__
from sgas.authz import engine
from sgas.server import config, messages, manifest, topresource
from sgas.database.postgresql import database as pgdatabase, hostscale
from sgas.viewengine import viewdefinition, viewresource


# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

DEFAULT_PORT = 6180



class ConfigurationError(Exception):
    """
    Raised when the server is configured wrong.
    """



def createSite(cfg, log, db, authorizer, views, mfst):

    tr = topresource.TopResource(authorizer)
    
    for plugin in cfg.options(config.PLUGINS):
        section = "plugin:%s" % plugin
        if not section in cfg.sections(): 
            log.msg("Plugin: %s can't be loaded :-( section %s is missing" % (plugin, section))
            continue
        if not config.PLUGIN_PACKAGE in cfg.options(section):
            log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_PACKAGE, section))
            continue
        if not config.PLUGIN_CLASS in cfg.options(section):
            log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_PACKAGE, section))
            continue
        if not config.PLUGIN_NAME in cfg.options(section):
            log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_NAME, section))
            continue
        if not config.PLUGIN_ID in cfg.options(section):
            log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_ID, section))
            continue
        
        log.msg("Loading %s.%s" % (cfg.get(section,config.PLUGIN_PACKAGE),cfg.get(section,config.PLUGIN_CLASS)))
              
        # import module
        pluginModule = __import__(cfg.get(section,config.PLUGIN_PACKAGE),globals(),locals(),[cfg.get(section,config.PLUGIN_CLASS)])
        # Create class
        pluginClass = getattr(pluginModule,cfg.get(section,config.PLUGIN_CLASS))
        # Instansiate object 
        pluginObj = pluginClass(db, authorizer, views, mfst)
        # register
        tr.registerService(pluginObj, cfg.get(section,config.PLUGIN_ID), ((cfg.get(section,config.PLUGIN_NAME), cfg.get(section,config.PLUGIN_ID)),) )
        

    root = resource.Resource()
    root.putChild('sgas', tr)

    site = server.Site(root)
    site.log = lambda *args : None
    return site



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

    mfst = manifest.Manifest()
    mfst.setProperty('start_time', time.asctime())
    if cfg.has_option(config.SERVER_BLOCK, config.WLCG_CONFIG_FILE):
        mfst.setProperty('wlcg_config_file', cfg.get(config.SERVER_BLOCK, config.WLCG_CONFIG_FILE))

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

    # get scale factors
    scale_factors = {}
    for hostname in cfg.options(config.SCALE_BLOCK):
        try:
            scale_factors[hostname] = cfg.getfloat(config.SCALE_BLOCK, hostname)
        except ValueError:
            log.msg('Invalid scale factor value for entry: %s' % hostname, system='sgas.Setup')

    hs = hostscale.HostScaleFactorUpdater(db, scale_factors)
    hs.setServiceParent(db)

    # http site
    views = viewdefinition.buildViewList(cfg)
    site = createSite(cfg, log, db, authorizer, views, mfst)
    
    # read auth file.
    authorizer.initAuthzFile()

    # application
    application = service.Application("sgas")

    log.setServiceParent(application)
    db.setServiceParent(application)

    internet.TCPServer(port or DEFAULT_PORT, site, interface='localhost').setServiceParent(application)

    log.msg('SGAS %s twistd application created, starting server.' % __version__, system='sgas.Setup')

    return application


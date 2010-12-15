"""
Server-setup logic
"""
from twisted.python import log
from twisted.application import internet, service
from twisted.web import resource, server

from sgas import __version__
from sgas.authz import engine
from sgas.server import config, topresource, insertresource, viewresource, queryresource
from sgas.database.postgresql import database as pgdatabase, hostscale
from sgas.viewengine import viewdefinition


# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

DEFAULT_PORT = 6180



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
    site.log = lambda *args : None
    return site



def createSGASServer(config_file=DEFAULT_CONFIG_FILE, port=None):

    cfg = config.readConfig(config_file)

    # check for whitelist and fail if it is there
    # a bit harsh, but the config needs to be updated for the system to work
    if cfg.get(config.SERVER_BLOCK, config.HOSTNAME_CHECK_WHITELIST):
        raise ConfigurationError('Whitelist no longer supported, use "insert:all" in sgas.authz')

    # some friendly messages from your service configuration
    if cfg.get(config.SERVER_BLOCK, config.HOSTKEY):
        log.msg('Option: hostkey can be removed from config file (no longer optional)', system='sgas.Setup')
    if cfg.get(config.SERVER_BLOCK, config.HOSTCERT):
        log.msg('Option:hostcert can be removed from config file (no longer optional)', system='sgas.Setup')
    if cfg.get(config.SERVER_BLOCK, config.CERTDIR):
        log.msg('Option: certdir can be removed from config file (no longer optional)', system='sgas.Setup')
    if cfg.get(config.SERVER_BLOCK, config.REVERSE_PROXY):
        log.msg('Option: reverse_proxy can be removed from config file (no longer optional)', system='sgas.Setup')

    # check depth
    try:
        check_depth = cfg.getint(config.SERVER_BLOCK, config.HOSTNAME_CHECK_DEPTH)
    except ValueError: # in case casting goes wrong
        raise ConfigurationError('Configured check depth is invalid')

    # authz
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
    site = createSite(db, authorizer, views)

    # application
    application = service.Application("sgas")

    db.setServiceParent(application)

    internet.TCPServer(port or DEFAULT_PORT, site, interface='localhost').setServiceParent(application)

    log.msg('SGAS %s twistd application created, starting server.' % __version__, system='sgas.Setup')

    return application


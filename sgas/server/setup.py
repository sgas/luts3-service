"""
Server-setup logic
"""
import os.path

from twisted.application import internet, service
from twisted.web import resource, server

from sgas.common import couchdb
from sgas.server import config, database, ssl, authz, \
                        topresource, insertresource, recordidresource, viewresource, staticresource
from sgas.viewengine import customview, chunks


# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

TCP_PORT = 6180
SSL_PORT = 6143



class ConfigurationError(Exception):
    """
    Raised when the server is configured wrong.
    """



def createSite(ur_db, authz, web_files_path):

    rr = insertresource.InsertResource(ur_db, authz)
    rr.putChild('recordid', recordidresource.RecordIDResource(ur_db))

    vr = viewresource.ViewTopResource(ur_db, authz)

    tr = topresource.TopResource(authz)
    tr.registerService(rr, 'ur', (('Registration', 'ur'),('RecordIDQuery', 'ur/recordid/{recordid}')))
    tr.registerService(vr, 'view', (('View', 'view'),))

    sr = staticresource.StaticResource(web_files_path)

    root = resource.Resource()
    root.putChild('sgas', tr)
    root.putChild('static', sr)

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

    cdb = couchdb.Database(cfg.get(config.SERVER_BLOCK, config.DB))

    view_specs = {}
    for block in cfg.sections():
        if block.startswith(config.VIEW_PREFIX):
            view_name = block.split(':',1)[-1]
            view_specs[view_name] = dict(cfg.items(block))

    chunk_manager = None
    ci_design = cfg.get(config.SERVER_BLOCK, config.COREINFO_DESIGN, None)
    ci_view   = cfg.get(config.SERVER_BLOCK, config.COREINFO_VIEW, None)
    if ci_design and ci_view:
        chunk_manager = chunks.InformationChunkManager(cdb, ci_design, ci_view)
    views = dict([ (view_name,customview.createCustomView(view_name, view_cfg)) for view_name, view_cfg in view_specs.items() ])
    ur_db = database.UsageRecordDatabase(cdb, chunk_manager, views)

    az = authz.Authorizer(cfg.get(config.SERVER_BLOCK, config.AUTHZ_FILE))
    web_files_path = cfg.get(config.SERVER_BLOCK, config.WEB_FILES)

    site = createSite(ur_db, az, web_files_path)

    # setup application
    application = service.Application("sgas")

    if use_ssl:
        hostkey  = cfg.get(config.SERVER_BLOCK, config.HOSTKEY)
        hostcert = cfg.get(config.SERVER_BLOCK, config.HOSTCERT)
        certdir  = cfg.get(config.SERVER_BLOCK, config.CERTDIR)
        cf = ssl.ContextFactory(key_path=hostkey, cert_path=hostcert, ca_dir=certdir)
        internet.SSLServer(port or SSL_PORT, site, cf).setServiceParent(application)
    else:
        internet.TCPServer(port or TCP_PORT, site).setServiceParent(application)

    return application


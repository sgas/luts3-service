"""
Server-setup logic
"""

from twisted.application import internet, service
from twisted.web import resource, server

from sgas.server import config, couchdb, database, view, ssl, authz, \
                        topresource, insertresource, recordidresource, viewresource, staticresource


# -- constants

DEFAULT_CONFIG_FILE = '/etc/sgas.conf'

TCP_PORT = 6180
SSL_PORT = 6143


def createSGASServer(config_file=DEFAULT_CONFIG_FILE, use_ssl=True, port=None):

    cfg = config.readConfig(config_file)

    cdb = couchdb.Database(cfg.get(config.SERVER_BLOCK, config.DB))

    view_specs = {}
    for block in cfg.sections():
        if block.startswith(config.VIEW_PREFIX):
            view_name = block.split(':',1)[-1]
            view_specs[view_name] = dict(cfg.items(block))

    views = dict([ (view_name,view.createView(view_name, view_cfg)) for view_name, view_cfg in view_specs.items() ])

    ur_db = database.UsageRecordDatabase(cdb, views)

    az = authz.Authorizer(cfg.get(config.SERVER_BLOCK, config.AUTHZ_FILE))

    rr = insertresource.InsertResource(ur_db, az)
    rr.putChild('recordid', recordidresource.RecordIDResource(ur_db))

    vr = viewresource.ViewResource(ur_db, az)

    tr = topresource.TopResource(az)
    tr.registerService(rr, 'ur', (('Registration', 'ur'),('RecordIDQuery', 'ur/recordid/{recordid}')))
    tr.registerService(vr, 'view', (('View', 'view'),))

    sr = staticresource.StaticResource(cfg.get(config.SERVER_BLOCK, config.WEB_FILES))

    root = resource.Resource()
    root.putChild('sgas', tr)
    root.putChild('static', sr)

    site = server.Site(root)

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


"""
UsageRecord view resource.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.database import error as dberror
from sgas.authz import rights as authzrights, ctxsetchecker
from sgas.server import config, resourceutil
from sgas.viewengine import html, pagebuilder, adminmanifest, machineview, rights

from sgas.viewengine import viewdefinition
from sgas.viewengine import manifest

import time

# generic error handler
def handleViewError(error, request, view_name):
    error_msg = error.getErrorMessage()
    if error.check(dberror.DatabaseUnavailableError):
        error.printTraceback()
        log.err(error, system='sgas.ViewResource')
        request.setResponseCode(503)
        error_msg = 'Database is currently unavailable, please try again later'
    else:
        log.err(error, system='sgas.ViewResource')
        request.setResponseCode(500)

    request.write(error_msg)
    request.finish()

PLUGIN_CFG_BLOCK = "plugin:view"     
WLCG_CONFIG_FILE = 'wlcg_config_file'


class ViewTopResource(resource.Resource):
    
    PLUGIN_ID   = 'view'
    PLUGIN_NAME = 'View'

    def __init__(self, cfg, db, authorizer):
        resource.Resource.__init__(self)
        self.urdb = db
        self.authorizer = authorizer
        authorizer.addChecker(rights.ACTION_VIEW, ctxsetchecker.AnySetChecker)
        authorizer.rights.addActions(rights.ACTION_VIEW)
        authorizer.rights.addOptions(rights.ACTION_VIEW,[ authzrights.OPTION_ALL ])
        authorizer.rights.addContexts(rights.ACTION_VIEW,[ rights.CTX_VIEW, rights.CTX_VIEWGROUP ])
        
        self.views = viewdefinition.buildViewList(cfg)
        
        mfst = manifest.Manifest()
        mfst.setProperty('start_time', time.asctime())
    
        if cfg.has_option(PLUGIN_CFG_BLOCK, WLCG_CONFIG_FILE):
            mfst.setProperty('wlcg_config_file', cfg.get(PLUGIN_CFG_BLOCK, WLCG_CONFIG_FILE))

        self.putChild('adminmanifest', adminmanifest.AdminManifestResource(self.urdb, authorizer, mfst))
        self.putChild('machines', machineview.MachineListView(self.urdb, authorizer, mfst))
        self.putChild('custom', CustomViewTopResource(self.urdb, authorizer, self.views))
        
        if mfst.hasProperty(WLCG_CONFIG_FILE):
            from sgas.viewengine import wlcgview
            self.putChild('wlcg', wlcgview.WLCGView(db, authorizer, mfst))


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)
        return self.renderStartPage(request, subject)


    def renderStartPage(self, request, identity):

        ib = 4 * ' '

        body =''
        body += 2*ib + '<h3>SGAS View Page</h3>\n'
        body += 2*ib + '<p>\n'
        body += 2*ib + '<div>Identity: %(identity)s</div>\n' % {'identity': identity }
        body += 2*ib + '<p>\n'
        body += 2*ib + '<div><a href=view/adminmanifest>Administrators Manifest</a></div>\n'
        body += 2*ib + '<p>\n'
        body += 2*ib + '<div><a href=view/machines>Machine list</a></div>\n'

        if 'wlcg' in self.children:
            body += 2*ib + '<p>\n'
            body += 2*ib + '<div><a href=view/wlcg>WLCG Views</a></div>\n'

        body += 2*ib + '<p> &nbsp; <p>\n'

        if self.views:
            body += 2*ib + '<h4>Custom views</h4>\n'

        for view in self.views:
            body += 2*ib + '<div><a href=view/custom/%s>%s</a></div>\n' % (view.view_name, view.caption)
            body += 2*ib + '<p>'
        if not self.views:
            body += 2*ib + '<div>No views defined in configuration file. See docs/views in the documentation for how specify views.</div>\n'

        request.write(html.HTML_VIEWBASE_HEADER % {'title': 'View startpage'} )
        request.write(body)
        request.write(html.HTML_VIEWBASE_FOOTER)
        request.finish()
        return server.NOT_DONE_YET



class CustomViewTopResource(resource.Resource):

    def __init__(self, urdb, authorizer, views):
        resource.Resource.__init__(self)
        self.urdb = urdb
        self.authorizer = authorizer

        self.views = views

        for view in self.views:
            self.putChild(view.view_name, GraphRenderResource(view, urdb, authorizer))



class GraphRenderResource(resource.Resource):

    def __init__(self, view, urdb, authorizer):
        resource.Resource.__init__(self)
        self.view = view
        self.urdb = urdb
        self.authorizer = authorizer


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)
        # authZ check
        ctx = [ (rights.CTX_VIEW, self.view.view_name) ] + [ (rights.CTX_VIEWGROUP, vg) for vg in self.view.view_groups ]
        if self.authorizer.isAllowed(subject, rights.ACTION_VIEW, context=ctx):
            return self.renderView(request)

        # access not allowed
        request.write('<html><body>Access to view %s not allowed for %s</body></html>' % (self.view.view_name, subject))
        request.finish()
        return server.NOT_DONE_YET


    def renderView(self, request):

        def gotResult(rows):

            # twisted web sets content-type to text/html per default
            page_body = pagebuilder.buildViewPage(self.view, rows)

            request.write(html.HTML_VIEWGRAPH_HEADER % {'title': self.view.caption} )
            request.write(page_body)
            request.write(html.HTML_VIEWGRAPH_FOOTER)
            request.finish()

        d = self.urdb.query(self.view.query)
        d.addCallback(gotResult)
        d.addErrback(handleViewError, request, self.view.view_name)
        return server.NOT_DONE_YET


"""
View definition module. Parser and representation of a view.
Part of the SGAS view engine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""


from twisted.python import log

from sgas.server import config
from sgas.viewengine import viewcore

# view options
VIEW_PREFIX      = 'view:'
VIEW_GROUP       = 'viewgroup'
VIEW_TYPE        = 'type'
VIEW_QUERY       = 'query'
VIEW_DESCRIPTION = 'description'
VIEW_DRAWTABLE   = 'drawtable'
VIEW_DRAWGRAPH   = 'drawgraph'

class ViewDefinition:

    def __init__(self, view_type, view_name, view_groups, query, caption=None, drawtable=None, drawgraph=None):
        self.view_type   = view_type
        self.view_name   = view_name
        self.view_groups = view_groups
        self.query       = query
        self.caption     = caption
        self.drawtable   = drawtable
        self.drawgraph   = drawgraph



def buildViewList(cfg):

    views = []

    for block in cfg.sections():
        if block.startswith(VIEW_PREFIX):
            view_name = block.split(':',1)[-1]
            view_args = dict(cfg.items(block))
            view = createViewDefinition(view_name, view_args)
            views.append(view)

    return views



def createViewDefinition(view_name, view_config):

    view_type   = None
    view_groups = []
    query       = None
    caption     = None
    drawtable   = None
    drawgraph   = None

    for key, value in view_config.items():
        if key == VIEW_TYPE:
            if not value in viewcore.VIEW_TYPES:
                raise config.ConfigurationError('Invalid view type: %s' % value)
            view_type = value

        elif key == VIEW_GROUP:
            view_groups = [ group.strip() for group in value.split(',') ]

        elif key == VIEW_QUERY:
            query = value

        elif key == VIEW_DESCRIPTION:
            caption = value

        elif key == VIEW_DRAWTABLE:
            drawtable = readBoolean(value)

        elif key == VIEW_DRAWGRAPH:
            drawgraph = readBoolean(value)

        else:
             log.msg("Unknown view definition key: %s" % key, system='sgas.ViewDefinition')

    if view_name in (None, ''):
        raise config.ConfigurationError('Missing or empty view name for view definition')
    if view_type in (None, ''):
        raise config.ConfigurationError('Missing or empty view type for view definition')
    if query in (None, ''):
        raise config.ConfigurationError('Missing or empty query for view definition')

    return ViewDefinition(view_type, view_name, view_groups, query, caption, drawtable, drawgraph)



def readBoolean(value):
    if value.lower() in ('true', '1'):
        return True
    elif value.lower() in ('false', '0'):
        return False

    raise config.ConfigurationError("Invalid boolean value (%s)" % value)


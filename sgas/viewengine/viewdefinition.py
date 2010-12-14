"""
View definition module. Parser and representation of a view.
Part of the SGAS view engine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""


from twisted.python import log

from sgas.server import config



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
        if block.startswith(config.VIEW_PREFIX):
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
        if key == config.VIEW_TYPE:
            view_type = value

        elif key == config.VIEW_GROUP:
            view_groups = [ group.strip() for group in value.split(',') ]

        elif key == config.VIEW_QUERY:
            query = value

        elif key == config.VIEW_DESCRIPTION:
            caption = value

        elif key == config.VIEW_DRAWTABLE:
            drawtable = readBoolean(value)

        elif key == config.VIEW_DRAWGRAPH:
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


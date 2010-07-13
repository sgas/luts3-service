"""
View definition module. Parser and representation of a view.
Part of the SGAS view engine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""


from twisted.python import log

from sgas.server import config



class ViewDefinition:

    def __init__(self, view_type, view_name, query, caption=None, drawtable=None, drawgraph=None):
        self.view_type = view_type
        self.view_name = view_name
        self.query     = query
        self.caption   = caption
        self.drawtable = drawtable
        self.drawgraph = drawgraph



def createViewDefinition(view_name, view_config):

    view_type   = None
    query       = None
    caption     = None
    drawtable   = None
    drawgraph   = None

    for key, value in view_config.items():
        if key == config.VIEW_TYPE:
            view_type = value

        elif key == config.VIEW_QUERY:
            query = value

        elif key == config.VIEW_DESCRIPTION:
            caption = value

        elif key == config.DRAWTABLE:
            drawtable = readBoolean(value)

        elif key == config.VIEW_DRAWGRAPH:
            drawgraph = readBoolean(value)

        else:
             log.msg("Unknown view definition key: %s" % key, system='sgas.view')

    for cfg_value in [ view_name, view_type, query ]:
        if cfg_value in (None, ''):
            raise config.ConfigurationError("Missing or empty value for view definition: %s" % value)

    return ViewDefinition(view_type, view_name, query, caption, drawtable, drawgraph)



def readBoolean(value):
    if value.lower() in ('true', '1'):
        return True
    elif value.lower() in ('false', '0'):
        return False

    raise config.ConfigurationError("Invalid boolean value (%s)" % value)


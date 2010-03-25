"""
customview module

can convert data from a customly defined couchdb view to data suitable for
putting in tables.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""


from twisted.python import log

from sgas.server import config
from sgas.viewengine import dataprocess



class CustomViewDefinition:

    def __init__(self, view_name, db_design, db_view, description=None, filter=None, postprocess=None):
        self.view_name   = view_name
        self.db_design   = db_design
        self.db_view     = db_view
        self.description = description
        self.filter_     = filter or (lambda : (None, None))
        self.postprocess = postprocess or []


    def filter(self):
        return self.filter_()


    def process(self, rows):
        for pp in self.postprocess:
            rows = [ pp(key, value) for key, value in rows ]
        return rows



def createCustomView(view_name, view_config):

    db_design   = None
    db_view     = None
    description = None
    filter      = None
    postprocess = None

    for key, value in view_config.items():
        if key == config.VIEW_DESIGN:
            db_design = value
        elif key == config.VIEW_NAME:
            db_view = value
        elif key == config.VIEW_DESCRIPTION:
            description = value
        elif key == config.VIEW_FILTER:
            try:
                filter = dataprocess.FILTERS[value]
            except KeyError:
                raise config.ConfigurationError("Invalid filter (%s)" % value)
        elif key == config.VIEW_POSTPROCESS:
            postprocess = []
            for pp_name in value.split(','):
                try:
                    postprocess.append(dataprocess.POST_PROCESSORS[pp_name])
                except KeyError:
                    raise config.ConfigurationError("Invalid post processor (%s)" % pp_name)
        else:
             log.msg("Unknown view definition key: %s" % key, system='sgas.view')

    return CustomViewDefinition(
        view_name,
        db_design,
        db_view,
        description=description,
        filter=filter,
        postprocess=postprocess
    )


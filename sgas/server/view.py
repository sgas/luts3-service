"""
UsageRecord view resource.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

import time

from twisted.python import log

from sgas.server import config


SECONDS_PER_WEEK = 7 * 24 * 60 * 60


def swap(l,x,y):
    l[x] = (l[x], l[y])
    l[y] = l[x][0]
    l[x] = l[x][1]
    return l


FILTERS = {
    'last_week' :
     lambda : ('["' + time.strftime('%Y %m %d', time.gmtime(time.time() - SECONDS_PER_WEEK)) + '",{}]', None)
}

POST_PROCESSORS = {
    'seconds_to_days'    : lambda key, value : (key, round(value / (24 * 3600.0) , 1)),
    'seconds_to_hours'   : lambda key, value : (key, round(value / (     3600.0) , 1)),
    'seconds_to_minutes' : lambda key, value : (key, round(value / (       60.0))),
    'flip12'             : lambda key, value : (swap(key, 0, 1), value),
}


class ViewDefinition:

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



def createView(view_name, view_config):

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
                filter = FILTERS[value]
            except KeyError:
                raise config.ConfigurationError("Invalid filter (%s)" % value)
        elif key == config.VIEW_POSTPROCESS:
            postprocess = []
            for pp_name in value.split(','):
                try:
                    postprocess.append(POST_PROCESSORS[pp_name])
                except KeyError:
                    raise config.ConfigurationError("Invalid post processor (%s)" % pp_name)
        else:
             log.msg("Unknown view definition key: %s" % key, system='sgas.view')

    return ViewDefinition(view_name, db_design, db_view, description=description, filter=filter,
                          postprocess=postprocess)


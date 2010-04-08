"""
Various utility functions for the viewresource, mainly to help with custom
view url tracking, rendering, and parsing.
"""


from sgas.viewengine import chunkprocess


GROUP = 'group'
DATE  = 'date'

URL_O_DATERES = 'dateres'

GROUPS = ['user', 'host', 'vo']
DATE_RESOLUTIONS = { 'collapse':0, 'year':1, 'month':2, 'day':3 }
VO_RESOLUTIONS = [ 0, 1, 2 ]


DEFAULT_GROUP = {
    'user' : 'host',
    'host' : 'vo',
    'vo'   : 'host'
}

DEFAULT_CLUSTER = {
    'user' : None,
    'host' : None,
    'vo'   : None
}

DEFAULT_RESOLUTION = {
    'vo'   : 1, # vo, no group/role
    'date' : 'month'
}


QUERY_DEFAULTS = {
    'group'      : DEFAULT_GROUP,
    'cluster'    : DEFAULT_CLUSTER
}


def parseDateResolution(value):
    if value in DATE_RESOLUTIONS:
        return value
    else:
        return DEFAULT_RESOLUTION[DATE]


def createQueryOptions(request_args, base_attr, resource):

    try:
        group = request_args[GROUP][-1]
    except KeyError:
        group  = QUERY_DEFAULTS[GROUP][base_attr]

    filter = { base_attr : resource }

    resolution = DEFAULT_RESOLUTION.copy()

    if URL_O_DATERES in request_args:
        url_date_res = parseDateResolution(request_args[URL_O_DATERES][-1])
    else:
        url_date_res = DEFAULT_RESOLUTION[DATE]

    resolution[DATE] = DATE_RESOLUTIONS[url_date_res]

    qo = chunkprocess.QueryOptions(group, filter=filter, resolution=resolution)
    return qo


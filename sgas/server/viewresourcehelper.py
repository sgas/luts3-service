"""
Various utility functions for the viewresource, mainly to help with custom
view url tracking, rendering, and parsing.
"""


from sgas.viewengine import chunkprocess


# url parsing constants
URL_O_GROUP   = 'group'
URL_O_CLUSTER = 'cluster'
URL_O_TIMERES = 'timeres'
URL_O_VORES   = 'vores'
URL_O_VALUES  = 'values'

URL_VALID_OPTIONS = {
    URL_O_GROUP   : ['user', 'host', 'vo'],
    URL_O_CLUSTER : ['user', 'host', 'vo'],
    URL_O_TIMERES : ['collapse', 'year', 'month', 'day'],
    URL_O_VORES   : ['collapse', 'vo', 'group', 'role'],
    URL_O_VALUES  : ['count', 'cputime', 'walltime']
}

URL_O_DEFAULTS = {
    'user' : {
        URL_O_GROUP   : 'host',
        URL_O_CLUSTER : None,
        URL_O_TIMERES : 'month',
        URL_O_VORES   : 'vo',
        URL_O_VALUES  : ['count', 'cputime', 'walltime']
    },
    'host' : {
        URL_O_GROUP   : 'vo',
        URL_O_CLUSTER : None,
        URL_O_TIMERES : 'month',
        URL_O_VORES   : 'vo',
        URL_O_VALUES  : ['count', 'cputime', 'walltime']
    },
    'vo' : {
        URL_O_GROUP   : 'host',
        URL_O_CLUSTER : None,
        URL_O_TIMERES : 'month',
        URL_O_VORES   : 'vo',
        URL_O_VALUES  : ['count', 'cputime', 'walltime']
    }
}

# stuff for query options

GROUP = 'group'
DATE  = 'date'

#GROUPS = ['user', 'host', 'vo']
TIME_RESOLUTIONS = { 'collapse':0, 'year':1, 'month':2, 'day':3 }
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



def parseURLParameters(request_args):

    url_options = {}

    print "pURL RA", request_args

    if URL_O_GROUP in request_args:
        group = request_args[URL_O_GROUP][-1]
        if group in URL_VALID_OPTIONS[URL_O_GROUP]:
            url_options[URL_O_GROUP] = group

    if URL_O_CLUSTER in request_args:
        cluster = request_args[URL_O_CLUSTER]
        if cluster in URL_VALID_OPTIONS[URL_O_CLUSTER]:
            url_options[URL_O_CLUSTER]

    if URL_O_TIMERES in request_args:
        time_res = request_args[URL_O_TIMERES][-1]
        if time_res in URL_VALID_OPTIONS[URL_O_TIMERES]:
            url_options[URL_O_TIMERES] = time_res

    if URL_O_VORES in request_args:
        vo_res = request_args[URL_O_VORES][-1]
        if vo_res in URL_VALID_OPTIONS[URL_O_VORES]:
            url_options[URL_O_VORES] = vo_res

    if URL_O_VALUES in request_args:
        values = request_args[URL_O_VALUES]
        for value in values:
            if value in URL_VALID_OPTIONS[URL_O_VALUES]:
                url_options.setdefault(URL_O_VALUES, []).append(value)

    return url_options



def parseTimeResolution(value):
    if value in TIME_RESOLUTIONS:
        return TIME_RESOLUTIONS[value]
    else:
        return TIME_RESOLUTIONS[DEFAULT_RESOLUTION[DATE]]


def createQueryOptions(url_options, base_attr, resource):

    print "QO", url_options
    group = url_options.get(URL_O_GROUP, QUERY_DEFAULTS[GROUP][base_attr])
    filter = { base_attr : resource }

    resolution = DEFAULT_RESOLUTION.copy()
    resolution[DATE] = parseTimeResolution(url_options.get(URL_O_TIMERES))
    print "QOR", resolution

    qo = chunkprocess.QueryOptions(group, filter=filter, resolution=resolution)
    return qo


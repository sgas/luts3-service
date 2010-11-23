"""
URL query parser for SGAS queryengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import time

from sgas.ext import isodate



MACHINE_NAME  = 'machine_name'
USER_IDENTITY = 'user_identity'
VO_NAME       = 'vo_name'
START_DATE    = 'start_date'
END_DATE      = 'end_date'

TIME_RESOLUTION = 'time_resolution'
DEFAULT_TIME_RESOLUTION = 'collapse'

UNDERSTOOD_QUERY_PARAMS    = [ MACHINE_NAME, USER_IDENTITY, VO_NAME, START_DATE, END_DATE, TIME_RESOLUTION ]

UNDERSTOOD_TIME_RESOLUTION = [ 'day', 'month', 'collapse' ] # collapse is default

DEFAULT_START_DATE      = '2000-01-01' # used if no start time is specified
DEFAULT_END_DATE_FORMAT = '%Y-%m-%d'



class QueryParseError(Exception):
    """
    Thrown if invalid arguments or data is given to the query parser.
    """


def isoDateTimeToPostgres(iso_date_time):

    date = isodate.parse_date(iso_date_time)
    return date.isoformat()



def parseURLArguments(request_args):

    # ensure all arguments are understood / allowed
    for query_field in request_args:
        if query_field not in UNDERSTOOD_QUERY_PARAMS:
            raise QueryParseError('Query field %s not understood/allowed.' % query_field)

    # only one start/end date
    if len(request_args.get(START_DATE, [])) > 1 or len(request_args.get(END_DATE, [])) > 1:
        raise QueryParseError('Only one value allowed for start and end time.')

    # only one time resolution
    if len(request_args.get(TIME_RESOLUTION, [])) > 1:
        raise QueryParseError('Only one value allowed for time resolution.')

    # only one vo name
    if len(request_args.get(VO_NAME, [])) > 1:
        raise QueryParseError('Only one value allowed for VO name.')

    # get arguments
    machine_names    = request_args.get(MACHINE_NAME)
    user_identities  = request_args.get(USER_IDENTITY)
    vo_name          = request_args.get(VO_NAME, [None])[0]
    iso_start_date   = request_args.get(START_DATE, [DEFAULT_START_DATE])[0]
    iso_end_date     = request_args.get(END_DATE, [time.strftime(DEFAULT_END_DATE_FORMAT)])[0]
    time_resolution  = request_args.get(TIME_RESOLUTION, [DEFAULT_TIME_RESOLUTION])[0]

    #print "ISO", iso_start_date, iso_end_date

    try:
        start_date = isoDateTimeToPostgres(iso_start_date)
        end_date = isoDateTimeToPostgres(iso_end_date)
        #print "SE", start_date, end_date
    except Exception, e:
        raise QueryParseError('Error parsing iso datetime: %s' % str(e))

    if not time_resolution in UNDERSTOOD_TIME_RESOLUTION:
        raise QueryParseError('Invalid time resolution parameter specified: %s' % time_resolution)

    query_args = {}

    if machine_names:
        query_args['machine_name'] = machine_names
    if user_identities:
        query_args['user_identity'] = user_identities
    if vo_name:
        query_args['vo_name'] = vo_name

    query_args['start_date'] = start_date
    query_args['end_date'] = end_date
    query_args['time_resolution'] = time_resolution

    return query_args



def filterAuthzParams(query_args):

    authz_params = query_args.copy()

    # remove elements which should not be authorized in authz engine
    authz_params.pop('start_date', None)
    authz_params.pop('end_date', None)
    authz_params.pop('time_resolution', None)

    return authz_params


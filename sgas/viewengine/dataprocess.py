"""
Various data processing functionality for views.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""

import time



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

SECONDS_TO_DAYS    = lambda value : (round(value / (24 * 3600.0) , 1))
SECONDS_TO_HOURS   = lambda value : (round(value / (     3600.0) , 1))

POST_PROCESSORS = {
    'seconds_to_days'    : lambda key, value : (key, SECONDS_TO_DAYS(value)),
    'seconds_to_hours'   : lambda key, value : (key, SECONDS_TO_HOURS(value)),
    'flip12'             : lambda key, value : (swap(key, 0, 1), value)
}


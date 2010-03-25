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

SECONDS_TO_DAYS    = lambda key, value : (key, round(value / (24 * 3600.0) , 1)),
SECONDS_TO_HOURS   = lambda key, value : (key, round(value / (     3600.0) , 1)),
SECONDS_TO_MINUTES = lambda key, value : (key, round(value / (       60.0))),
FLIP12             = lambda key, value : (swap(key, 0, 1), value),


POST_PROCESSORS = {
    'seconds_to_days'    : SECONDS_TO_DAYS,
    'seconds_to_hours'   : SECONDS_TO_HOURS,
    'seconds_to_minutes' : SECONDS_TO_MINUTES,
    'flip12'             : FLIP12
}


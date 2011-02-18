"""
URL parsing functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import time
import calendar



def getStartEndDates(request):

    def currentMonthStartDate():
        gmtime = time.gmtime()
        startdate = '%s-%02d-%s' % (gmtime.tm_year, gmtime.tm_mon, '01')
        return startdate

    def currentMonthEndDate():
        gmtime = time.gmtime()
        last_month_day = str(calendar.monthrange(gmtime.tm_year, gmtime.tm_mon)[1])
        enddate = '%s-%02d-%s' % (gmtime.tm_year, gmtime.tm_mon, last_month_day)
        return enddate

    if 'startdate' in request.args:
        startdate = request.args['startdate'][0].replace('-', '')
        if startdate == '':
            startdate = currentMonthStartDate().replace('-', '')
        elif len(startdate) == 8:
            pass
        elif len(startdate) == 6:
            startdate += '01'
        else: 
            raise baseview.ViewError('Invalid startdate parameter: %s' % request.args['startdate'][0])
        startdate = startdate[0:4] + '-' + startdate[4:6] + '-' + startdate[6:8]
    else:
        startdate = currentMonthStartDate()

    if 'enddate' in request.args:
        enddate = request.args['enddate'][0].replace('-', '')
        if enddate == '':
            enddate = currentMonthEndDate().replace('-', '')
        elif len(enddate) == 8:
            pass
        elif len(enddate) == 6:
            enddate += str(calendar.monthrange(int(enddate[0:4]), int(enddate[4:6]))[1])
        else:
            raise baseview.ViewError('Invalid enddate parameter: %s' % request.args['enddate'][0])
        enddate = enddate[0:4] + '-' + enddate[4:6] + '-' + enddate[6:8]
    else:
        enddate = currentMonthEndDate()

    return startdate, enddate


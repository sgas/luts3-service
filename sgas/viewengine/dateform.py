"""
Various date related functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import time
import datetime
import calendar

from sgas.viewengine import baseview, html



def parseStartEndDates(request):

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




def dayDelta(start_date, end_date):

    d_start = datetime.date(int(start_date[0:4]), int(start_date[5:7]),int(start_date[8:10]))
    d_end   = datetime.date(int(end_date[0:4]),   int(end_date[5:7]),  int(end_date[8:10]))
    days = (d_end - d_start).days + 1
    return days



def generateMonthFormOptions():

    # generate year-month options one year back
    month_options = ['']
    gmt = time.gmtime()
    for i in range(gmt.tm_mon-12, gmt.tm_mon+1):
        if i <= 0:
            month_options.append('%i-%02d' % (gmt.tm_year - 1, 12 + i) )
        elif i > 0:
            month_options.append('%i-%02d' % (gmt.tm_year, i) )

    return month_options



def createMonthSelectorForm(baseurl, start_date_option, end_date_option):

    month_options = generateMonthFormOptions()

    sel1 = html.createSelector('Start month', 'startdate', month_options, start_date_option)
    sel2 = html.createSelector('End month', 'enddate', month_options, end_date_option)
    selector_form = html.createSelectorForm(baseurl, [sel1, sel2] )

    return selector_form







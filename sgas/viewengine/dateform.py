"""
Various date related functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import time
import math
import datetime
import calendar

from sgas.viewengine import baseview, html



def _currentMonthStartDate():
    gmtime = time.gmtime()
    startdate = '%s-%02d-%s' % (gmtime.tm_year, gmtime.tm_mon, '01')
    return startdate



def _currentMonthEndDate():
    gmtime = time.gmtime()
    last_month_day = str(calendar.monthrange(gmtime.tm_year, gmtime.tm_mon)[1])
    enddate = '%s-%02d-%s' % (gmtime.tm_year, gmtime.tm_mon, last_month_day)
    return enddate



def parseDate(request):

    if b'date' in request.args:
        date = request.args[b'date'][0].decode('utf-8').replace('-', '')
        if date == '':
            date = _currentMonthStartDate().replace('-', '')
        elif len(date) == 8:
            pass
        elif len(date) == 6:
            date += '01'
        else: 
            raise baseview.ViewError('Invalid date parameter: %s' % request.args['date'][0])
        date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    else:
        date = _currentMonthStartDate()

    return date



def parseStartEndDates(request):
    if b'startdate' in request.args:
        startdate = request.args[b'startdate'][0].decode('utf-8').replace('-', '')
        if startdate == '':
            startdate = _currentMonthStartDate().replace('-', '')
        elif len(startdate) == 8:
            pass
        elif len(startdate) == 6:
            startdate += '01'
        else: 
            raise baseview.ViewError('Invalid startdate parameter: %s' % request.args['startdate'][0])
        startdate = startdate[0:4] + '-' + startdate[4:6] + '-' + startdate[6:8]
    else:
        startdate = _currentMonthStartDate()

    if b'enddate' in request.args:
        enddate = request.args[b'enddate'][0].decode('utf-8').replace('-', '')
        if enddate == '':
            enddate = _currentMonthEndDate().replace('-', '')
        elif len(enddate) == 8:
            pass
        elif len(enddate) == 6:
            enddate += str(calendar.monthrange(int(enddate[0:4]), int(enddate[4:6]))[1])
        else:
            raise baseview.ViewError('Invalid enddate parameter: %s' % request.args['enddate'][0])
        enddate = enddate[0:4] + '-' + enddate[4:6] + '-' + enddate[6:8]
    else:
        enddate = _currentMonthEndDate()

    return startdate, enddate




def dayDelta(start_date, end_date):

    d_start = datetime.date(int(start_date[0:4]), int(start_date[5:7]),int(start_date[8:10]))
    d_end   = datetime.date(int(end_date[0:4]),   int(end_date[5:7]),  int(end_date[8:10]))
    days = (d_end - d_start).days + 1
    if days < 1:
        raise ValueError('Cannot calculate day delta for decreasing date span')
    return days



def generateMonthFormOptions():

    # generate year-month options for entire last year and all months this year
    month_options = ['']
    gmt = time.gmtime()
    for i in range(1,13):
        month_options.append('%i-%02d' % (gmt.tm_year - 1, i) )
    for i in range(1,gmt.tm_mon+1):
           month_options.append('%i-%02d' % (gmt.tm_year, i) )

    return month_options



def createMonthSelectorForm(baseurl, start_date_option, end_date_option, buttons=None):

    month_options = generateMonthFormOptions()

    sel1 = html.createSelector('Start month', 'startdate', month_options, start_date_option)
    sel2 = html.createSelector('End month', 'enddate', month_options, end_date_option)
    selector_form = html.createSelectorForm(baseurl, [ sel1, sel2 ], buttons)

    return selector_form


# quarter stuff from here


def _monthToQuarter(m):

    return int(math.ceil(m/3.0))



def currentYearQuart():
    gmt = time.gmtime()
    return gmt.tm_year, _monthToQuarter(gmt.tm_mon)



def currentQuarter():
    year, quart = currentYearQuart()
    quarter = str(year) + '-Q' + str(quart)
    return quarter



def parseQuarter(quarter):

    if len(quarter) != 7 or quarter[4:6] != '-Q':
        raise ValueError('Invalid quarter value: %s' % quarter)

    year = int(quarter[:4])
    quart = int(quarter[6])
    return year, quart



def parseRequestQuarter(request):

    if b'quarter' in request.args:
        quarter = request.args[b'quarter'][0].decode('utf-8')
    else:
        quarter = currentQuarter()

    if quarter == '':
        quarter = currentQuarter()

    try:
        year, quart = parseQuarter(quarter)
    except ValueError as e:
        raise baseview.ViewError(str(e))

    return year, quart



def quarterStartEndDates(year, quart):

    base_dates = {
        1 : ('%i-01-01', '%i-03-31'),
        2 : ('%i-04-01', '%i-06-30'),
        3 : ('%i-07-01', '%i-09-30'),
        4 : ('%i-10-01', '%i-12-31')
    }

    if not quart in base_dates:
        raise ValueError('Invalid quart specified (%s)' % quart)

    sd, ed = base_dates[quart]

    # if quarter is current quarter, make enddate today
    gmt = time.gmtime()
    if year == gmt.tm_year and _monthToQuarter(gmt.tm_mon) == quart:
        ed = '%i-' + '%02d-%02d' % (gmt.tm_mon, gmt.tm_mday)

    return (sd % year, ed % year)



def generateFormQuarters():

    gmt = time.gmtime()
    quarters = []
    for i in range(1,5):
        quarters.append('%i-Q%i' % (gmt.tm_year - 1, i))
    for i in range (1, _monthToQuarter(gmt.tm_mon)+1):
        quarters.append('%i-Q%i' % (gmt.tm_year, i))
    return quarters



def generateQuarterFormOptions():

    quarter_options = [''] + generateFormQuarters()
    return quarter_options



def createQuarterSelectorForm(baseurl, quarter):

    quarter_options = generateQuarterFormOptions()

    sel = html.createSelector('Quarter', 'quarter', quarter_options, quarter)
    selector_form = html.createSelectorForm(baseurl, [sel] )
    return selector_form


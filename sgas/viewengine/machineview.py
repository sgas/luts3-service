"""
Machine view. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import time
import calendar

from twisted.internet import defer
from twisted.web import server

from sgas.authz import rights
from sgas.server import httphtml, resourceutil
from sgas.viewengine import htmltable, baseview


# Various stat queries

QUERY_MACHINE_LIST = """SELECT machine_name FROM machinename;"""

QUERY_MACHINE_MANIFEST = """
SELECT
    min(insert_time)    AS first_record_registration,
    max(insert_time)    AS last_record_registration,
    min(execution_time) AS first_job_start,
    max(execution_time) AS last_job_termination,
    count(distinct user_identity) AS distinct_users,
    count(distinct vo_name) AS distinct_projects,
    sum(n_jobs)         AS jobs
FROM uraggregated
WHERE machine_name = %s;
"""


QUERY_EXECUTED_JOBS_PER_DAY = """
SELECT
    d.dates,
    sum(n_jobs)
FROM (SELECT current_date - s as dates FROM generate_series(0,10) as s) as d
  LEFT OUTER JOIN uraggregated ON (d.dates = uraggregated.execution_time::date AND machine_name = %s)
GROUP BY d.dates ORDER BY d.dates;
"""

QUERY_TOP10_PROJECTS = """
SELECT
    vo_name,
    (sum(walltime) / 24)::integer AS walltime,
    CASE WHEN sum(walltime)::integer = 0 THEN null ELSE ((sum(cputime) * 100.0) / sum(walltime))::integer END AS efficiancy,
    sum(n_jobs)
FROM uraggregated
WHERE
    machine_name = %s AND
    execution_time >= %s AND
    execution_time <= %s
GROUP BY vo_name
ORDER BY walltime DESC
LIMIT 20;
"""

QUERY_TOP20_USERS = """
SELECT
    user_identity,
    (sum(walltime) / 24)::integer as walltime,
    CASE WHEN sum(walltime)::integer = 0 THEN null ELSE ((sum(cputime) * 100.0) / sum(walltime))::integer END as efficiancy,
    sum(n_jobs)
FROM uraggregated
WHERE
    machine_name = %s AND
    execution_time >= %s AND
    execution_time <= %s
GROUP BY user_identity
ORDER BY walltime DESC
LIMIT 20;
"""



class MachineListView(baseview.BaseView):


    def getChild(self, path, request):
        return MachineView(self.urdb, self.authorizer, self.manifest, path)


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        if self.authorizer.hasRelevantRight(subject, rights.ACTION_VIEW):
            d = self.retrieveMachineList()
            d.addCallbacks(self.renderMachineList, self.renderErrorPage, callbackArgs=(request,), errbackArgs=(request,))
            return server.NOT_DONE_YET
        else:
            return self.renderAuthzErrorPage(request, 'machine list', subject)


    def retrieveMachineList(self):

        d = self.urdb.query(QUERY_MACHINE_LIST)
        return d


    def renderMachineList(self, results, request):

        machines = [ r[0] for r in results]
        title = 'Machine list'

        request.write(httphtml.HTML_VIEWBASE_HEADER % {'title': title})
        request.write('<h3>%s</h3>\n' % title)
        request.write('<p>\n')

        # service info
        for machine in machines:
            request.write('<div><a href=machines/%s>%s</a></div>\n' % (machine, machine))
            request.write('<p>\n')

        request.write(httphtml.HTML_VIEWBASE_FOOTER)

        request.finish()
        return server.NOT_DONE_YET


    def renderErrorPage(self, error, request):

        request.write('Error rendering page: %s' % str(error))
        request.finish()
        return server.NOT_DONE_YET




class MachineView(baseview.BaseView):


    def __init__(self, urdb, authorizer, manifest, machine_name):

        baseview.BaseView.__init__(self, urdb, authorizer, manifest)
        self.machine_name = machine_name


    def getStartEndDates(self, request):

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


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ 'machine', self.machine_name ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'machine view for %s' % self.machine_name, subject)
        # access allowed
        start_date, end_date = self.getStartEndDates(request)
        d = self.retrieveMachineInfo(start_date, end_date)
        d.addCallbacks(self.renderMachineView, self.renderErrorPage, callbackArgs=(request,), errbackArgs=(request,))
        return server.NOT_DONE_YET


    def retrieveMachineInfo(self, start_date, end_date):

        defs = []
        for query in [ QUERY_MACHINE_MANIFEST, QUERY_EXECUTED_JOBS_PER_DAY ]:
            d = self.urdb.query(query, (self.machine_name,) )
            defs.append(d)

        for query in [ QUERY_TOP10_PROJECTS, QUERY_TOP20_USERS ]:
            d = self.urdb.query(query, [self.machine_name, start_date, end_date] )
            defs.append(d)

        dl = defer.DeferredList(defs)
        return dl


    def renderMachineView(self, results, request):

        machine_manifest = results[0][1][0]
        jobs_per_day     = results[1][1]
        top_projects     = results[2][1]
        top_users        = results[3][1]

        first_record_registration   = machine_manifest[0]
        last_record_registration    = machine_manifest[1]
        first_job_start             = machine_manifest[2]
        last_job_termination        = machine_manifest[3]
        distinct_users              = machine_manifest[4]
        distinct_projects           = machine_manifest[5]
        n_jobs                      = machine_manifest[6]

        e_batches = [ e[0] for e in jobs_per_day ] # dates
        e_groups  = [ 'jobs' ]
        e_matrix  = dict( [ ((e[0], e_groups[0]), e[1]) for e in jobs_per_day ] )
        executed_table = htmltable.createHTMLTable(e_matrix, e_batches, e_groups, base_indent=4)

        stats_batches = [ 'Walltime days', 'Efficiency', 'Number of jobs' ]

        p_groups = [ e[0] for e in top_projects ] # user list
        p_dict_elements = []
        p_dict_elements += [ ((stats_batches[0], p[0]), p[1]) for p in top_projects ]
        p_dict_elements += [ ((stats_batches[1], p[0]), p[2]) for p in top_projects ]
        p_dict_elements += [ ((stats_batches[2], p[0]), p[3]) for p in top_projects ]
        p_matrix = dict(p_dict_elements)
        project_table = htmltable.createHTMLTable(p_matrix, stats_batches, p_groups, base_indent=4)

        u_groups = [ e[0] for e in top_users ] # user list
        u_dict_elements = []
        u_dict_elements += [ ((stats_batches[0], u[0]), u[1]) for u in top_users ]
        u_dict_elements += [ ((stats_batches[1], u[0]), u[2]) for u in top_users ]
        u_dict_elements += [ ((stats_batches[2], u[0]), u[3]) for u in top_users ]
        u_matrix = dict(u_dict_elements)
        user_table = htmltable.createHTMLTable(u_matrix, stats_batches, u_groups, base_indent=4)

        title = 'Machine view for %s' % self.machine_name

        request.write(httphtml.HTML_VIEWBASE_HEADER % {'title': title})
        request.write( httphtml.createTitle(title) )

        start_date_option = request.args.get('startdate', [''])[0]
        end_date_option   = request.args.get('enddate', [''])[0]

        month_options = [ '', '2010-09', '2010-10', '2010-11', '2010-12', '2011-01', '2011-02' ]

        request.write('''
    <form name="input" action="%s" method="get">
        Start month: <select name=startdate>''' % self.machine_name)
        for mo in month_options:
            if start_date_option == mo:
                request.write('''            <option selected>%s</option>\n''' % mo)
            else:
                request.write('''            <option>%s</option>\n''' % mo)
        request.write('''        </select>

        &nbsp; &nbsp;

        End month: <select name=enddate>''')
        for mo in month_options:
            if end_date_option == mo:
                request.write('''            <option selected>%s</option>\n''' % mo)
            else:
                request.write('''            <option>%s</option>\n''' % mo)
        request.write('''        </select>

        &nbsp; &nbsp;

    <input type="submit" value="Submit" />
    </form>\n\n''')

        request.write( httphtml.createSectionTitle('Manifest') )
        request.write( httphtml.createParagraph('First record registration: %s' % first_record_registration) )
        request.write( httphtml.createParagraph('Last record registration: %s' % last_record_registration) )
        request.write( httphtml.createParagraph('First job start: %s' % first_job_start) )
        request.write( httphtml.createParagraph('Last job termination: %s' % last_job_termination) )
        request.write( httphtml.createParagraph('Distinct users: %s' % distinct_users) )
        request.write( httphtml.createParagraph('Distinct projects: %s' % distinct_projects) )
        request.write( httphtml.createParagraph('Number of jobs: %s' % n_jobs) )
        request.write( httphtml.SECTION_BREAK )

        request.write( httphtml.createSectionTitle('Executed jobs in the last ten days') )
        request.write( executed_table )
        request.write( httphtml.SECTION_BREAK)

        request.write( httphtml.createSectionTitle('Top 10 projects in the selected range (current month if not selected)') )
        request.write( project_table )
        request.write( httphtml.SECTION_BREAK )

        request.write( httphtml.createSectionTitle('Top 20 users in the selected date range (current month if not selected)') )
        request.write( user_table )
        request.write( httphtml.P + '\n' )

        request.write(httphtml.HTML_VIEWBASE_FOOTER)

        request.finish()
        return server.NOT_DONE_YET



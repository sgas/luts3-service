"""
Machine view. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import time

from twisted.internet import defer
from twisted.web import server

from sgas.server import resourceutil
from sgas.viewengine import html, htmltable, dateform, baseview, rights


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

        body = html.HTML_VIEWBASE_HEADER % {'title': title}
        body += '<h3>%s</h3>\n<p>\n' % title

        # service info
        for machine in machines:
            body += '<div><a href=machines/%s>%s</a></div>\n<p>\n' % (machine, machine)

        body += html.HTML_VIEWBASE_FOOTER
        request.write(body.encode('utf-8'))

        request.finish()
        return server.NOT_DONE_YET


    def renderErrorPage(self, error, request):

        request.write(('Error rendering page: %s' % str(error)).encode('utf-8'))
        request.finish()
        return server.NOT_DONE_YET




class MachineView(baseview.BaseView):


    def __init__(self, urdb, authorizer, manifest, machine_name):

        baseview.BaseView.__init__(self, urdb, authorizer, manifest)
        self.machine_name = machine_name.decode('utf-8')


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ ('machine', self.machine_name) ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'machine view for %s' % self.machine_name, subject)
        # access allowed
        start_date, end_date = dateform.parseStartEndDates(request)
        print(start_date,end_date)
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

        start_date_option = request.args.get(b'startdate', [b''])[0].decode('utf-8')
        end_date_option   = request.args.get(b'enddate', [b''])[0].decode('utf-8')

        print("start_date_option: ", start_date_option)

        # generate year-month options one year back
        month_options = ['']
        gmt = time.gmtime()
        for i in range(gmt.tm_mon-12, gmt.tm_mon+1):
            if i <= 0:
                month_options.append('%i-%02d' % (gmt.tm_year - 1, 12 + i) )
            elif i > 0:
                month_options.append('%i-%02d' % (gmt.tm_year, i) )

        sel1 = html.createSelector('Start month', 'startdate', month_options, start_date_option)
        sel2 = html.createSelector('End month', 'enddate', month_options, end_date_option)
        selector_form = html.createSelectorForm(self.machine_name, [sel1, sel2] )

        if start_date_option or end_date_option:
            range_text = 'selected date range'
        else:
            range_text = 'current month'

        # create page
        page = html.HTML_VIEWBASE_HEADER % {'title': title}
        page += html.createTitle(title)
        page += '\n' + selector_form + '\n'

        if not (start_date_option or end_date_option):
        # skip manifest / inserts if date range is selected
            page += html.createSectionTitle('Manifest')
            page += html.createParagraph('First record registration: %s' % first_record_registration)
            page += html.createParagraph('Last record registration: %s' % last_record_registration)
            page += html.createParagraph('First job start: %s' % first_job_start)
            page += html.createParagraph('Last job termination: %s' % last_job_termination)
            page += html.createParagraph('Distinct users: %s' % distinct_users)
            page += html.createParagraph('Distinct projects: %s' % distinct_projects)
            page += html.createParagraph('Number of jobs: %s' % n_jobs)
            page += html.SECTION_BREAK

            page += html.createSectionTitle('Executed jobs in the last ten days')
            page += executed_table
            page += html.SECTION_BREAK

        page += html.createSectionTitle('Top 10 projects for the %s' % range_text)
        page += project_table
        page += html.SECTION_BREAK

        page += html.createSectionTitle('Top 20 users for the %s' % range_text)
        page += user_table
        page += html.P + '\n'

        page += html.HTML_VIEWBASE_FOOTER
        request.write(page.encode('utf-8'))

        request.finish()
        return server.NOT_DONE_YET



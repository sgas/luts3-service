"""
Machine view. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.internet import defer
from twisted.web import server

from sgas.authz import rights
from sgas.server import httphtml, resourceutil
from sgas.viewengine import htmltable, baseview


# Various stat queries

QUERY_MACHINE_LIST = """SELECT machine_name FROM machinename;"""

QUERY_EXECUTED_JOBS_PER_DAY = """
SELECT d.dates, sum(n_jobs)
FROM (SELECT current_date - s as dates FROM generate_series(0,8) as s) as d
  LEFT OUTER JOIN uraggregated ON (d.dates = uraggregated.execution_time::date AND machine_name = %s)
GROUP BY d.dates ORDER BY d.dates;
"""

QUERY_TOP10_PROJECTS = """
SELECT vo_name, sum(walltime)::integer as walltime, sum(n_jobs)
FROM uraggregated
WHERE execution_time > current_date - interval '1 month' AND machine_name = %s
GROUP BY vo_name
ORDER BY walltime DESC
LIMIT 20;
"""

QUERY_TOP20_USERS = """
SELECT user_identity, sum(walltime)::integer as walltime, sum(n_jobs)
FROM uraggregated
WHERE execution_time > current_date - interval '1 month' AND machine_name = %s
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


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ 'machine', self.machine_name ]
        if self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            d = self.retrieveMachineInfo()
            d.addCallbacks(self.renderMachineView, self.renderErrorPage, callbackArgs=(request,), errbackArgs=(request,))
            return server.NOT_DONE_YET
        else:
            return self.renderAuthzErrorPage(request, 'machine view for %s' % self.machine_name, subject)


    def retrieveMachineInfo(self):

        defs = []
        for query in [ QUERY_EXECUTED_JOBS_PER_DAY, QUERY_TOP10_PROJECTS, QUERY_TOP20_USERS ]:
            d = self.urdb.query(query, (self.machine_name,))
            defs.append(d)

        dl = defer.DeferredList(defs)
        return dl


    def renderMachineView(self, results, request):

        jobs_per_day = results[0][1]
        top_projects = results[1][1]
        top_users    = results[2][1]

        e_batches = [ e[0] for e in jobs_per_day ] # dates
        e_groups  = [ 'jobs' ]
        e_matrix  = dict( [ ((e[0], e_groups[0]), e[1]) for e in jobs_per_day ] )
        executed_table = htmltable.createHTMLTable(e_matrix, e_batches, e_groups)

        p_batches = [ 'Walltime hours', 'Number of jobs' ]
        p_groups = [ e[0] for e in top_projects ] # user list
        p_dict_elements = []
        p_dict_elements += [ ((p_batches[0], p[0]), p[1]) for p in top_projects ]
        p_dict_elements += [ ((p_batches[1], p[0]), p[2]) for p in top_projects ]
        p_matrix = dict(p_dict_elements)
        project_table = htmltable.createHTMLTable(p_matrix, p_batches, p_groups)

        u_batches = [ 'Walltime hours', 'Number of jobs' ]
        u_groups = [ e[0] for e in top_users ] # user list
        u_dict_elements = []
        u_dict_elements += [ ((u_batches[0], u[0]), u[1]) for u in top_users ]
        u_dict_elements += [ ((u_batches[1], u[0]), u[2]) for u in top_users ]
        u_matrix = dict(u_dict_elements)
        user_table = htmltable.createHTMLTable(u_matrix, u_batches, u_groups)

        title = 'Machine view for %s' % self.machine_name

        request.write(httphtml.HTML_VIEWBASE_HEADER % {'title': title})
        request.write('<h3>%s</h3>\n' % title)
        request.write('<p> &nbsp; <p>\n')

        request.write('<h5>Executed jobs in the last eight days</h5>\n')
        request.write(executed_table)
        request.write('<p> &nbsp; <p>\n')

        request.write('<h5>Top 10 projects in the last month</h5>\n')
        request.write(project_table)
        request.write('<p> &nbsp; <p>\n')

        request.write('<h5>Top 20 users for the last month</h5>\n')
        request.write(user_table)
        request.write('<p>\n')

        request.write(httphtml.HTML_VIEWBASE_FOOTER)

        request.finish()
        return server.NOT_DONE_YET



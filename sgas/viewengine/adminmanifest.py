"""
Administrators manifest. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.internet import defer
from twisted.web import server

from sgas import __version__ as sgas_version
from sgas.authz import rights
from sgas.server import resourceutil
from sgas.viewengine import html, htmltable, baseview


# Various stat queries

DB_STATS_QUERY = """
SELECT sum(n_jobs) as count,
       pg_size_pretty(pg_database_size(current_database()))
FROM uraggregated, pg_class
WHERE relname = 'usagedata' group by relpages;
"""

INSERTS_PER_DAY = """
SELECT d.dates, sum(n_jobs)
  FROM (SELECT current_date - s as dates FROM generate_series(0,8) as s) as d
    LEFT OUTER JOIN uraggregated ON (d.dates = uraggregated.insert_time::date)
GROUP BY d.dates ORDER BY d.dates;
"""

MACHINES_INSERTED_TODAY = """
SELECT DISTINCT machine_name FROM uraggregated WHERE insert_time = current_date;
"""

STALE_MACHINES_TWO_MONTHS = """
SELECT DISTINCT machine_name FROM uraggregated WHERE insert_time > current_date - interval '2 months'
  EXCEPT SELECT DISTINCT machine_name FROM uraggregated WHERE insert_time > current_date - interval '3 days';
"""



class AdminManifestResource(baseview.BaseView):


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'admin') ]
        if self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            d = self.retrieveDatabaseStats()
            d.addCallbacks(self.renderAdminManifestPage, self.renderErrorPage, callbackArgs=(request,), errbackArgs=(request,))
            return server.NOT_DONE_YET
        else:
            return self.renderAuthzErrorPage(request, 'administrators manifest', subject)


    def retrieveDatabaseStats(self):

        defs = []

        # get database info and schedule future rendering
        for query in [ DB_STATS_QUERY, INSERTS_PER_DAY, MACHINES_INSERTED_TODAY, STALE_MACHINES_TWO_MONTHS ]:
            d = self.urdb.query(query)
            defs.append(d)

        dl = defer.DeferredList(defs)
        dl.addCallback(self.gotDatabaseResults)
        return dl


    def gotDatabaseResults(self, results):

        all_success = all( [ r[0] for r in results ] )
        if not all_success:
            raise Exception('db error')

        results = [ r[1] for r in results ]
        return results


    def renderAdminManifestPage(self, results, request):

        #print "rAM", results
        db_stats                = results[0]
        inserts_per_day         = results[1]
        machines_inserted_today = [ r[0] for r in results[2] ]
        stale_machines          = [ r[0] for r in results[3] ]

        #print db_stats
        n_jobs, db_size = db_stats[0]

        # create table over insertions/date
        batches = [ i[0] for i in inserts_per_day ]
        groups = [ 'records' ]
        matrix = dict( [ ((i[0], groups[0]), i[1]) for i in inserts_per_day ] )
        inserts_table = htmltable.createHTMLTable(matrix, batches, groups)

        manifest_props = self.manifest.getAllProperties()

        request.write(html.HTML_VIEWBASE_HEADER % {'title': 'Administrators Manifest'})
        request.write('<h3>Administrators Manifest</h3>\n')
        request.write('<p> &nbsp; <p>\n\n')

        # service info
        request.write('<h4>Service</h4>\n')
        request.write('SGAS version: %s\n' % sgas_version)
        request.write('<p>\n')
        request.write('Up since: %s\n' % manifest_props['start_time'])
        request.write('<p> &nbsp; <p>\n\n')

        # database info
        request.write('<h4>Database</h4>\n')
        request.write('Number of usage records: %s\n' % n_jobs)
        request.write('<p>\n')
        request.write('Database size: %s\n' % db_size)
        request.write('<p> &nbsp; <p>\n\n')

        # registration info
        request.write('<h4>Registrations</h4>\n')

        request.write('<h5>Registrations over the last 8 days:</h5>\n')
        request.write(inserts_table)
        request.write('<p> &nbsp; <p>\n\n')

        request.write('<h5>Machines which has registered records today:</h5>\n')
        request.write('<p>\n')
        for mn in machines_inserted_today:
            request.write('%s\n' % mn)
            request.write('<p>\n')
        if not machines_inserted_today:
            request.write('(none)\n')
            request.write('<p>\n')
        request.write('<p> &nbsp; <p>\n\n')

        request.write('<h5>Machines which has registered records within the last two months, but not in the last three days:</h5>\n')
        request.write('<p>\n')
        for mn in stale_machines:
            request.write('%s\n' % mn)
            request.write('<p>\n')
        if not stale_machines:
            request.write('(none)\n')
            request.write('<p>\n')
        request.write('<p>\n')

        request.write(html.HTML_VIEWBASE_FOOTER)

        request.finish()
        return server.NOT_DONE_YET


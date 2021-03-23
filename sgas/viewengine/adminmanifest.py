"""
Administrators manifest. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.internet import defer
from twisted.web import server

from sgas import __version__ as sgas_version
from sgas.server import resourceutil
from sgas.viewengine import html, htmltable, baseview, rights


# Various stat queries

DB_STATS_QUERY = """
SELECT * FROM
    (SELECT sum(n_jobs) FROM uraggregated_data) as urcount,
    (SELECT count(*) FROM storagedata) as srcount,
    (SELECT pg_size_pretty(pg_database_size(current_database()))) AS dbsize;
"""

INSERTS_PER_DAY = """
SELECT ur_insert.dates, ur_insert.sum, NULLIF( sr_insert.count, 0) FROM
  ( SELECT d.dates, sum(n_jobs)
    FROM (SELECT current_date - s as dates FROM generate_series(0,8) as s) as d
    LEFT OUTER JOIN uraggregated_data ON (d.dates = uraggregated_data.insert_time::date)
    GROUP BY d.dates ORDER BY d.dates) AS ur_insert
LEFT OUTER JOIN
  ( SELECT d.dates, count(storagedata.*)
    FROM (SELECT current_date - s as dates FROM generate_series(0,8) as s) as d
    LEFT OUTER JOIN storagedata ON (d.dates = storagedata.insert_time::date)
    GROUP BY d.dates ORDER BY d.dates) AS sr_insert
    ON (ur_insert.dates = sr_insert.dates);
"""

MACHINES_UR_INSERTED_RECENT = """
SELECT DISTINCT machine_name FROM uraggregated WHERE (insert_time = current_date OR insert_time = current_date-1) AND
                                                     generate_time AT TIME ZONE 'UTC'  > current_timestamp - interval '24 hours';
"""

MACHINES_SR_INSERTED_RECENT = """
SELECT DISTINCT storage_system FROM storagerecords WHERE insert_time AT TIME ZONE 'UTC' > current_timestamp - interval '24 hours';
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
        for query in [ DB_STATS_QUERY, INSERTS_PER_DAY, MACHINES_UR_INSERTED_RECENT, MACHINES_SR_INSERTED_RECENT, STALE_MACHINES_TWO_MONTHS ]:
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
        db_stats            = results[0]
        inserts_per_day     = results[1]
        machines_ur_insert  = [ r[0] for r in results[2] ]
        machines_sr_insert  = [ r[0] for r in results[3] ]
        stale_machines      = [ r[0] for r in results[4] ]

        #print db_stats
        n_ur_recs, n_sr_recs, db_size = db_stats[0]

        # create table over insertions/date
        batches = [ i[0] for i in inserts_per_day ]
        groups = [ 'Usage records', 'Storage records' ]
        matrix = dict( [ ((i[0], groups[0]), i[1]) for i in inserts_per_day ] +
                       [ ((i[0], groups[1]), i[2]) for i in inserts_per_day ] )
        inserts_table = htmltable.createHTMLTable(matrix, batches, groups)

        manifest_props = self.manifest.getAllProperties()

        body = html.HTML_VIEWBASE_HEADER % {'title': 'Administrators Manifest'}


        body += '<h3>Administrators Manifest</h3>\n'
        body += '<p> &nbsp; <p>\n\n'

        # service info
        body += '<h4>Service</h4>\n'
        body += 'SGAS version: %s\n' % sgas_version
        body += '<p>\n'
        body += 'Up since: %s\n' % manifest_props['start_time']
        body += '<p> &nbsp; <p>\n\n'

        # database info
        body += '<h4>Database</h4>\n'
        body += 'Number of usage records: %s\n' % n_ur_recs
        body += '<p>\n'
        body += 'Number of storage records: %s\n' % n_sr_recs
        body += '<p>\n'
        body += 'Database size: %s\n' % db_size
        body += '<p> &nbsp; <p>\n\n'

        # registration info
        body += '<h4>Registrations</h4>\n'

        body += '<h5>Registrations over the last 8 days</h5>\n'
        body += inserts_table
        body += '<p> &nbsp; <p>\n\n'

        body += '<h5>Machines which has registered usage records in the last 24 hours</h5>\n'
        body += '<p>\n'
        for mn in machines_ur_insert:
            body += '%s\n' % mn
            body += '<p>\n'
        if not machines_ur_insert:
            body += '(none)\n'
            body += '<p>\n'
        body += '<p> &nbsp; <p>\n\n'

        body += '<h5>Machines which has registered storage records in the last 24 hours</h5>\n'
        body += '<p>\n'
        for mn in machines_sr_insert:
            body += '%s\n' % mn
            body += '<p>\n'
        if not machines_sr_insert:
            body += '(none)\n'
            body += '<p>\n'
        body += '<p> &nbsp; <p>\n\n'

        body += '<h5>Machines which has registered records within the last two months, but NOT in the last three days</h5>\n'
        body += '<h5>This is a list of "potentially" problematic machines</h5>\n'
        body += '<p>\n'
        for mn in stale_machines:
            body += '%s\n' % mn
            body += '<p>\n'
        if not stale_machines:
            body += '(none)\n'
            body += '<p>\n'
        body += '<p> &nbsp; <p>\n\n'

        body += html.HTML_VIEWBASE_FOOTER
        request.write(body.encode('utf-8'))

        request.finish()
        return server.NOT_DONE_YET


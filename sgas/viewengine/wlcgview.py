"""
WLCG View. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import time
import json

from wlcgsgas import query as wlcgquery, dataprocess

from twisted.web import server

from sgas.authz import rights
from sgas.server import resourceutil
from sgas.viewengine import html, htmltable, dateform, baseview


# Mapping for more readable column names
COLUMN_NAMES = {
    dataprocess.YEAR                    : 'Year',
    dataprocess.MONTH                   : 'Month',
    dataprocess.TIER                    : 'Tier',
    dataprocess.HOST                    : 'Host',
    dataprocess.VO_NAME                 : 'VO',
    dataprocess.VO_GROUP                : 'VO Group',
    dataprocess.VO_ROLE                 : 'VO Role',
    dataprocess.USER                    : 'User',
    dataprocess.N_JOBS                  : 'Job count',
    dataprocess.CPU_TIME                : 'CPU time',
    dataprocess.WALL_TIME               : 'Wall time',
    dataprocess.KSI2K_CPU_TIME          : 'KSI2K CPU time',
    dataprocess.KSI2K_WALL_TIME         : 'KSI2K wall time',
    dataprocess.EFFICIENCY              : 'Job efficiency',
    dataprocess.CPU_EQUIVALENTS         : 'CPU node equivalents',
    dataprocess.WALL_EQUIVALENTS        : 'Wall node equivalents',
    dataprocess.KSI2K_CPU_EQUIVALENTS   : 'KSI2K CPU node equivalents',
    dataprocess.KSI2K_WALL_EQUIVALENTS  : 'KSI2K Wall node equivalents'
}



class WLCGView(baseview.BaseView):


    def __init__(self, urdb, authorizer, manifest):

        baseview.BaseView.__init__(self, urdb, authorizer, manifest)

        self.subview = {
            'machine'   : ('WLCG machine view', WLCGMachineView(self.urdb, self.authorizer, self.manifest, 'machine')),
            'vo'        : ('WLCG VO view',      WLCGVOView(self.urdb, self.authorizer, self.manifest, 'vo')),
            'user'      : ('WLCG User view',    WLCGUserView(self.urdb, self.authorizer, self.manifest, 'user')),
            'tier'      : ('WLCG tier view',    WLCGTierView(self.urdb, self.authorizer, self.manifest, 'tier')),
            'fulltier'  : ('WLCG full tier view', WLCGFullTierView(self.urdb, self.authorizer, self.manifest, 'fulltier')),
            'tiersplit' : ('WLCG tier-machine split view', WLCGTierMachineSplitView(self.urdb, self.authorizer, self.manifest, 'tiersplit'))
        }

    def getChild(self, path, request):
        if path in self.subview:
            return self.subview[path][1]
        else:
            # no such resource
            return baseview.BaseView.getChild(self, path, request)


    def render_GET(self, request):

        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg') ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG view', subject)

        # access allowed
        request.write(html.HTML_VIEWBASE_HEADER % {'title': 'WLCG Views'})
        request.write( html.createTitle('WLCG Views') )
        for view_url, ( description, _ ) in self.subview.items():
            request.write('<div><a href=wlcg/%s>%s</a></div>\n' % (view_url, description))
            request.write( html.P )
        request.write(html.HTML_VIEWBASE_FOOTER)
        request.finish()
        return server.NOT_DONE_YET



class WLCGBaseView(baseview.BaseView):

    collapse = []
    columns = []
    split = None
    tier_based = False

    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)

        wlcg_config = json.load(open(mfst.getProperty('wlcg_config_file')))
        self.tier_mapping = wlcg_config['tier-mapping']
        self.tier_shares  = wlcg_config['tier-ratio']
        self.default_tier = 'NDGF-T1'


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg') ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG %s view' % self.path, subject)

        # access allowed
        start_date, end_date = dateform.parseStartEndDates(request)
        t_query_start = time.time()
        d = self.retrieveWLCGData(start_date, end_date)
        d.addCallbacks(self.renderWLCGViewPage, self.renderErrorPage, callbackArgs=(request, start_date, end_date, t_query_start), errbackArgs=(request,))
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, start_date, end_date):

        d = self.urdb.query(wlcgquery.WLCG_QUERY, (start_date, end_date))
        return d


    def renderWLCGViewPage(self, wlcg_data, request, start_date, end_date, t_query_start):

        t_query = time.time() - t_query_start

        days = dateform.dayDelta(start_date, end_date)

        # massage data
        #print "L1", len(wlcg_data)
        t_dataprocess_start = time.time()
        wlcg_records = dataprocess.rowsToDicts(wlcg_data)
        wlcg_records = dataprocess.addMissingScaleValues(wlcg_records)
        wlcg_records = dataprocess.collapseFields(wlcg_records, self.collapse)
        if self.tier_based:
            wlcg_records = dataprocess.tierMergeSplit(wlcg_records, self.tier_mapping, self.tier_shares, self.default_tier)
            if self.split != dataprocess.TIER:
                wlcg_records = dataprocess.collapseFields(wlcg_records, ( dataprocess.HOST, ) )
        # information on ops vo does not add any value
        wlcg_records = [ rec for rec in wlcg_records if rec[dataprocess.VO_NAME] != 'ops' ]
        wlcg_records = dataprocess.addEffiencyProperty(wlcg_records)
        wlcg_records = dataprocess.addEquivalentProperties(wlcg_records, days)

        if self.split is None:
            sk = lambda key : dataprocess.sortKey(key, field_order=self.columns)
            wlcg_records = sorted(wlcg_records, key=sk)
        else:
            split_records = dataprocess.splitRecords(wlcg_records, dataprocess.TIER)

        #print "L2", len(wlcg_records)
        t_dataprocess = time.time() - t_dataprocess_start

        if self.split is None:
            table_content = self.createTable(wlcg_records, self.columns)
        else:
            table_content = ''
            for split_attr, records in split_records.items():
                sk = lambda key : dataprocess.sortKey(key, field_order=self.columns)
                records = sorted(records, key=sk)
                table = self.createTable(records, self.columns)
                table_content += html.createParagraph(split_attr) + table + html.SECTION_BREAK

        start_date_option = request.args.get('startdate', [''])[0]
        end_date_option   = request.args.get('enddate', [''])[0]

        title = 'WLCG %s view' % self.path
        selector_form = dateform.createMonthSelectorForm(self.path, start_date_option, end_date_option)
        range_text = html.createParagraph('Date range: %s - %s (%s days)' % (start_date, end_date, days))

        request.write( html.HTML_VIEWBASE_HEADER % {'title': title} )
        request.write( html.createTitle(title) )
        request.write( html.createParagraph(selector_form) )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph(range_text) )
        request.write( table_content )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph('Query time: %s' % round(t_query, 2)) )
        request.write( html.createParagraph('Data process time: %s' % round(t_dataprocess, 2)) )
        request.write( html.HTML_VIEWBASE_FOOTER )

        request.finish()
        return server.NOT_DONE_YET



    def createTable(self, records, columns):

        def formatValue(v):
            if type(v) is float:
                return int(v)
            else:
                return v

        row_names = range(0, len(records))
        elements = []
        for rn in row_names:
            for c in columns:
                elements.append( ((c, rn), formatValue(records[rn][c])) )
        matrix = dict(elements)
        table_markup = htmltable.createHTMLTable(matrix, columns, row_names, column_labels=COLUMN_NAMES, skip_base_column=True)

        return table_markup



class WLCGVOView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER )
    columns = [ dataprocess.VO_NAME, dataprocess.VO_ROLE, dataprocess.HOST, 
                dataprocess.N_JOBS, dataprocess.WALL_TIME, dataprocess.WALL_EQUIVALENTS,
                dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]


class WLCGTierView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER )
    columns = [ dataprocess.TIER, dataprocess.VO_NAME, dataprocess.VO_ROLE,
                dataprocess.N_JOBS, dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]
    tier_based = True



class WLCGFullTierView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH )
    columns = [ dataprocess.TIER, dataprocess.VO_NAME, dataprocess.VO_GROUP, dataprocess.VO_ROLE, dataprocess.USER,
                dataprocess.N_JOBS, dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]
    tier_based = True



class WLCGTierMachineSplitView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER )
    columns = [ dataprocess.HOST, dataprocess.VO_NAME, dataprocess.VO_ROLE,
                dataprocess.N_JOBS, dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]
    tier_based = True
    split = dataprocess.TIER


class WLCGMachineView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.VO_ROLE, dataprocess.USER )
    columns = [ dataprocess.HOST, dataprocess.VO_NAME, dataprocess.N_JOBS,
                dataprocess.WALL_TIME, dataprocess.WALL_EQUIVALENTS, dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]


class WLCGUserView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.HOST, dataprocess.VO_GROUP )
    columns = [ dataprocess.USER, dataprocess.VO_NAME, dataprocess.VO_ROLE, dataprocess.N_JOBS,
                dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]


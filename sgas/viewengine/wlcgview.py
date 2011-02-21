"""
WLCG View. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""

import json

from wlcgsgas import query as wlcgquery, dataprocess

from twisted.web import server

from sgas.authz import rights
from sgas.server import resourceutil
from sgas.viewengine import html, htmltable, dateform, baseview



class WLCGView(baseview.BaseView):


    def getChild(self, path, request):
        if path == 'machine':
            return WLCGMachineView(self.urdb, self.authorizer, self.manifest, path)
        elif path == 'vo':
            return WLCGVOView(self.urdb, self.authorizer, self.manifest, path)
        elif path == 'tier':
            return WLCGTierView(self.urdb, self.authorizer, self.manifest, path)
        elif path == 'user':
            return WLCGUserView(self.urdb, self.authorizer, self.manifest, path)
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
        request.write('<div><a href=wlcg/machine>WLCG machine view</a></div>\n')
        request.write( html.P )
        request.write('<div><a href=wlcg/vo>WLCG VO View</a></div>\n')
        request.write( html.P )
        request.write('<div><a href=wlcg/tier>WLCG tier view</a></div>\n')
        request.write( html.P )
        request.write('<div><a href=wlcg/user>WLCG user view</a></div>\n')
        request.write( html.P )
        request.write(html.HTML_VIEWBASE_FOOTER)

        request.finish()
        return server.NOT_DONE_YET



class WLCGBaseView(baseview.BaseView):

    collapse = []
    columns = []
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
        d = self.retrieveWLCGData(start_date, end_date)
        d.addCallbacks(self.renderWLCGViewPage, self.renderErrorPage, callbackArgs=(request, start_date, end_date), errbackArgs=(request,))
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, start_date, end_date):

        d = self.urdb.query(wlcgquery.WLCG_QUERY, (start_date, end_date))
        return d


    def renderWLCGViewPage(self, wlcg_data, request, start_date, end_date):

        def formatValue(v):
            if type(v) is float:
                return int(v)
            else:
                return v

        days = dateform.dayDelta(start_date, end_date)

        # massage data
        #print "L1", len(wlcg_data)
        wlcg_records = dataprocess.rowsToDicts(wlcg_data)
        wlcg_records = dataprocess.addMissingScaleValues(wlcg_records)
        wlcg_records = dataprocess.collapseFields(wlcg_records, self.collapse)
        if self.tier_based:
            wlcg_records = dataprocess.tierMergeSplit(wlcg_records, self.tier_mapping, self.tier_shares, self.default_tier)
        wlcg_records = dataprocess.addEffiencyProperty(wlcg_records)
        wlcg_records = dataprocess.addEquivalentProperties(wlcg_records, days)
        sk = lambda key : dataprocess.sortKey(key, field_order=self.columns)
        wlcg_records = sorted(wlcg_records, key=sk)
        #print "L2", len(wlcg_records)
        # information on ops vo does not add any value
        wlcg_records = [ rec for rec in wlcg_records if rec[dataprocess.VO_NAME] != 'ops' ]
        #print "L3", len(wlcg_records)

        row_names = range(0, len(wlcg_records))

        elements = []
        for rn in row_names:
            for c in self.columns:
                elements.append( ((c, rn), formatValue(wlcg_records[rn][c])) )
        matrix = dict(elements)
        table = htmltable.createHTMLTable(matrix, self.columns, row_names, skip_base_column=True)

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
        request.write( table )
        request.write( html.HTML_VIEWBASE_FOOTER )

        request.finish()
        return server.NOT_DONE_YET



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



class WLCGMachineView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.VO_ROLE, dataprocess.USER )
    columns = [ dataprocess.HOST, dataprocess.VO_NAME, dataprocess.N_JOBS,
                dataprocess.WALL_TIME, dataprocess.WALL_EQUIVALENTS, dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]


class WLCGUserView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.HOST, dataprocess.VO_GROUP )
    columns = [ dataprocess.USER, dataprocess.VO_NAME, dataprocess.VO_ROLE, dataprocess.N_JOBS,
                dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]


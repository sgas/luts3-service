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


def _formatValue(v):
    if type(v) is float:
        return int(v)
    else:
        return v


class WLCGView(baseview.BaseView):


    def __init__(self, urdb, authorizer, manifest):

        baseview.BaseView.__init__(self, urdb, authorizer, manifest)

        self.subview = {
            'machine'   : ('WLCG machine view', WLCGMachineView(self.urdb, self.authorizer, self.manifest, 'machine')),
            'vo'        : ('WLCG VO view',      WLCGVOView(self.urdb, self.authorizer, self.manifest, 'vo')),
            'user'      : ('WLCG User view',    WLCGUserView(self.urdb, self.authorizer, self.manifest, 'user')),
            'tier'      : ('WLCG tier view',    WLCGTierView(self.urdb, self.authorizer, self.manifest, 'tier')),
            'fulltier'  : ('WLCG full tier view', WLCGFullTierView(self.urdb, self.authorizer, self.manifest, 'fulltier')),
            'tiersplit' : ('WLCG tier-machine split view', WLCGTierMachineSplitView(self.urdb, self.authorizer, self.manifest, 'tiersplit')),
            'quarterly' : ('WLCG Quarterly view', WLCGQuarterlyView(self.urdb, self.authorizer, self.manifest, 'quarterly'))
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
        self.default_tier = wlcg_config['default-tier']


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

        sk = lambda key : dataprocess.sortKey(key, field_order=self.columns)
        if self.split is None:
            wlcg_records = sorted(wlcg_records, key=sk)
        else:
            split_records = dataprocess.splitRecords(wlcg_records, dataprocess.TIER)
            for split_attr, records in split_records.items():
                split_records[split_attr] = sorted(records, key=sk)

        #print "L2", len(wlcg_records)
        t_dataprocess = time.time() - t_dataprocess_start

        if self.split is None:
            table_content = self.createTable(wlcg_records, self.columns)
        else:
            table_content = ''
            for split_attr, records in split_records.items():
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

        row_names = range(0, len(records))
        elements = []
        for rn in row_names:
            for c in columns:
                elements.append( ((c, rn), _formatValue(records[rn][c])) )
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



## --- work area



class WLCGQuarterlyView(baseview.BaseView):

    # This view is rather different than the others, so it is its own class
    collapse = [ dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER ]

    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)

        wlcg_config = json.load(open(mfst.getProperty('wlcg_config_file')))
        self.tier_mapping = wlcg_config['tier-mapping']
        self.tier_shares  = wlcg_config['tier-ratio']
        self.default_tier = wlcg_config['default-tier']


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg') ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG Querterly view' % self.path, subject)

        # access allowed
        year, quart = dateform.parseQuarter(request)
        start_date, end_date = dateform.quarterStartEndDates(year, quart)
        days = dateform.dayDelta(start_date, end_date)

        t_query_start = time.time()
        d = self.retrieveWLCGData(start_date, end_date)
        d.addCallbacks(self.renderWLCGViewPage, self.renderErrorPage, callbackArgs=(request, days, t_query_start), errbackArgs=(request,))
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, start_date, end_date):

        d = self.urdb.query(wlcgquery.WLCG_QUERY, (start_date, end_date))
        return d


    def renderWLCGViewPage(self, wlcg_data, request, days, t_query_start):

        t_query = time.time() - t_query_start
        t_dataprocess_start = time.time()

        wlcg_records = dataprocess.rowsToDicts(wlcg_data)
        # information on ops and dteam vo does not add any value
        wlcg_records = [ rec for rec in wlcg_records if rec[dataprocess.VO_NAME] not in ('dteam', 'ops') ]

        # massage data
        wlcg_records = dataprocess.addMissingScaleValues(wlcg_records)
        wlcg_records = dataprocess.collapseFields(wlcg_records, self.collapse)
        wlcg_records = dataprocess.tierMergeSplit(wlcg_records, self.tier_mapping, self.tier_shares, self.default_tier)
        # role must be collapsed after split in order for the tier split to function
        wlcg_records = dataprocess.collapseFields(wlcg_records, [ dataprocess.VO_ROLE ] )

        sort_key = lambda key : dataprocess.sortKey(key, field_order=[ dataprocess.HOST] )
        split_records = dataprocess.splitRecords(wlcg_records, dataprocess.TIER)
        for split_attr, records in split_records.items():
            split_records[split_attr] = sorted(records, key=sort_key)

        t_dataprocess = time.time() - t_dataprocess_start

        # get tld groups
        hosts = set( [ rec[dataprocess.HOST] for rec in wlcg_records ] )
        tld_groups = {}
        for host in hosts:
            tld = host.split('.')[-1].upper()
            tld_groups.setdefault(tld, []).append(host)

        # create composite to-tier names
        vo_tiers = set()
        for rec in wlcg_records:
            tl = 't1' if 'T1' in rec[dataprocess.TIER] else 't2'
            vt = tl + '-' + rec[dataprocess.VO_NAME]
            rec[dataprocess.VO_NAME] = vt
            del rec[dataprocess.TIER] # same as collapsing afterwards
            vo_tiers.add(vt)

        TOTAL = 'Total'
        TIER_TOTAL = self.default_tier.split('-')[0].upper()

        # calculate total per site
        site_totals = dataprocess.collapseFields(wlcg_records, ( dataprocess.VO_NAME, ) )
        for r in site_totals:
            r[dataprocess.VO_NAME] = TOTAL

        # calculate total per country-tier
        country_tier_totals = [ r.copy() for r in wlcg_records ]
        for rec in country_tier_totals:
            rec[dataprocess.HOST] = rec[dataprocess.HOST].split('.')[-1].upper() + '-TOTAL'
            rec[dataprocess.USER] = 'FAKE'
        country_tier_totals = dataprocess.collapseFields(country_tier_totals, ( dataprocess.USER, ) )

        # calculate total per country
        country_totals = dataprocess.collapseFields(country_tier_totals, ( dataprocess.VO_NAME, ) )
        for rec in country_totals:
            rec[dataprocess.VO_NAME] = TOTAL

        # calculate total per tier-vo
        tier_vo_totals = dataprocess.collapseFields(wlcg_records, ( dataprocess.HOST, ) )
        for r in tier_vo_totals:
            r[dataprocess.HOST] = TIER_TOTAL

        # calculate total
        total = dataprocess.collapseFields(wlcg_records, ( dataprocess.HOST, dataprocess.VO_NAME ) )
        assert len(total) == 1, 'Records did not collapse into a single record when calculating grand total'
        total_record = total[0]
        total_record[dataprocess.HOST] = TIER_TOTAL
        total_record[dataprocess.VO_NAME] = TOTAL

        # put all calculated records together and add equivalents
        wlcg_records += site_totals
        wlcg_records += country_tier_totals
        wlcg_records += country_totals
        wlcg_records += tier_vo_totals
        wlcg_records += [ total_record ]
        wlcg_records = dataprocess.addEquivalentProperties(wlcg_records, days)

        # create table
        columns = sorted(vo_tiers)
        columns.append(TOTAL)

        row_names = []
        for tld in sorted(tld_groups):
            row_names += tld_groups[tld]
            row_names.append(tld + '-TOTAL')
        row_names.append(TIER_TOTAL)

        elements = []
        for row in row_names:
            for col in columns:
                for rec in wlcg_records:
                    if rec[dataprocess.HOST] == row and rec[dataprocess.VO_NAME] == col:
                        value = _formatValue(rec[dataprocess.KSI2K_WALL_EQUIVALENTS])
                        # hurrah for formatting
                        if row == TIER_TOTAL and col == TOTAL:
                            value = htmltable.StyledTableValue(value, bold=True, double_underlined=True)
                        elif (row.endswith('-TOTAL') and col == TOTAL) or row == TIER_TOTAL:
                            value = htmltable.StyledTableValue(value, bold=True, underlined=True)
                        elif row.endswith('-TOTAL') or row == TIER_TOTAL or col == TOTAL:
                            value = htmltable.StyledTableValue(value, bold=True)
                        elements.append( ((col,row), value))
                        break
                else:
                    elements.append( ((col,row), '') )

        matrix = dict(elements)
        table_content = htmltable.createHTMLTable(matrix, columns, row_names, column_labels=COLUMN_NAMES)

        # render page
        quarter = request.args.get('quarter', [''])[0]

        title = 'WLCG %s view' % self.path
        selector_form = dateform.createQuarterSelectorForm(self.path, quarter)
        range_text = html.createParagraph('Quarter: %s (%i days)' % (quarter or 'current', days))

        request.write( html.HTML_VIEWBASE_HEADER % {'title': title} )
        request.write( html.createTitle(title) )
        request.write( html.createParagraph(selector_form) )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph(range_text) )
        request.write( table_content )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph('Numbers are scaled node equivalents') )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph('Query time: %s' % round(t_query, 2)) )
        request.write( html.createParagraph('Data process time: %s' % round(t_dataprocess, 2)) )
        request.write( html.HTML_VIEWBASE_FOOTER )

        request.finish()
        return server.NOT_DONE_YET


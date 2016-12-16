"""
WLCG View. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
        Erik Edelmann <edelmann@csc.fi>
Copyright: Nordic Data Grid Facility (2011), Nordic e-Infrastructure Collaboration (2016)
"""

import time
import json

from wlcgsgas import query as wlcgquery, dataprocess

from twisted.web import server

from sgas.server import resourceutil, config
from sgas.viewengine import html, htmltable, dateform, baseview, rights


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
    dataprocess.HS06_CPU_TIME           : 'HS06 CPU hours',
    dataprocess.HS06_WALL_TIME          : 'HS06 wall hours',
    dataprocess.EFFICIENCY              : 'Job efficiency',
    dataprocess.CPU_EQUIVALENTS         : 'CPU node equivalents',
    dataprocess.WALL_EQUIVALENTS        : 'Wall node equivalents',
    dataprocess.KSI2K_CPU_EQUIVALENTS   : 'KSI2K CPU node equivalents',
    dataprocess.KSI2K_WALL_EQUIVALENTS  : 'KSI2K Wall node equivalents',
    dataprocess.HS06_CPU_EQUIVALENTS    : 'HS06 CPU node equivalents',
    dataprocess.HS06_WALL_EQUIVALENTS   : 'HS06 Wall node equivalents'
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
            't1summary': ('WLCG T1 Summary', WLCGT1SummaryView(self.urdb, self.authorizer, self.manifest, 't1summary')),
            'machine'   : ('WLCG machine view', WLCGMachineView(self.urdb, self.authorizer, self.manifest, 'machine')),
            'vooversight': ('WLCG VO oversight view', WLCGVOOversightView(self.urdb, self.authorizer, self.manifest, 'vooversight')),
            'machinepermonth'   : ('WLCG machine per month view', WLCGMachinePerMonthView(self.urdb, self.authorizer, self.manifest, 'machinepermonth')),
            'vo'        : ('WLCG VO view',      WLCGVOView(self.urdb, self.authorizer, self.manifest, 'vo')),
            'user'      : ('WLCG User view',    WLCGUserView(self.urdb, self.authorizer, self.manifest, 'user')),
            'tier'      : ('WLCG tier view',    WLCGTierView(self.urdb, self.authorizer, self.manifest, 'tier')),
            'fulltier'  : ('WLCG full tier view', WLCGFullTierView(self.urdb, self.authorizer, self.manifest, 'fulltier')),
            'tiersplit' : ('WLCG tier-machine split view', WLCGTierMachineSplitView(self.urdb, self.authorizer, self.manifest, 'tiersplit')),
            'oversight' : ('WLCG Oversight view', WLCGOversightView(self.urdb, self.authorizer, self.manifest, 'oversight')),
            'storage'   : ('WLCG Storage view', WLCGStorageView(self.urdb, self.authorizer, self.manifest, 'storage'))
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
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg'), (rights.CTX_VIEWGROUP, 'pub') ]
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
    viewgroup = 'pub'
    sort = staticmethod(sorted)

    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)

        wlcg_config = json.load(open(mfst.getProperty('wlcg_config_file')))
        self.tier_mapping = wlcg_config['tier-mapping']
        self.tier_shares  = wlcg_config['tier-ratio']
        self.hepspec06  = wlcg_config['hepspec06']
        self.default_tier = str(wlcg_config['default-tier'])


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg'), (rights.CTX_VIEWGROUP, self.viewgroup) ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG %s view' % self.path, subject)

        # access allowed
        start_date, end_date = dateform.parseStartEndDates(request)
        t_query_start = time.time()
        d = self.retrieveWLCGData(start_date, end_date)
        d.addCallback(self.renderWLCGViewPage, request, start_date, end_date, t_query_start)
        d.addErrback(self.renderErrorPage, request)
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
        wlcg_records = dataprocess.addMissingScaleValues(wlcg_records, self.hepspec06)
        wlcg_records = dataprocess.collapseFields(wlcg_records, self.collapse)
        if self.tier_based:
            wlcg_records = dataprocess.tierMergeSplit(wlcg_records, self.tier_mapping, self.tier_shares, self.default_tier)
            if not self.split:
                wlcg_records = dataprocess.collapseFields(wlcg_records, ( dataprocess.HOST, ) )
        # information on ops vo does not add any value
        wlcg_records = [ rec for rec in wlcg_records if rec[dataprocess.VO_NAME] != 'ops' ]
        wlcg_records = dataprocess.addEffiencyProperty(wlcg_records)
        wlcg_records = dataprocess.addEquivalentProperties(wlcg_records, days)

        sk = lambda key : dataprocess.sortKey(key, field_order=self.columns)
        if self.split is None:
            wlcg_records = self.sort(wlcg_records, key=sk)
        else:
            split_records = dataprocess.splitRecords(wlcg_records, self.split)
            for split_attr, records in split_records.items():
                split_records[split_attr] = self.sort(records, key=sk)

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
    viewgroup = 'restricted'



class WLCGTierMachineSplitView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER )
    columns = [ dataprocess.HOST, dataprocess.VO_NAME, dataprocess.VO_ROLE,
                dataprocess.N_JOBS, dataprocess.HS06_WALL_TIME, dataprocess.HS06_CPU_TIME, dataprocess.HS06_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]
    tier_based = True
    split = dataprocess.TIER


def sortAndSumByCountry(records, key):
    """
    Sort the records by country, and insert records representing sum over
    the countries, and append a total sum at the end.

    The argument 'key' is there for compatibility with the builtin function
    'sorted' only.

    """

    countryCode = lambda x: x[dataprocess.HOST].split(".")[-1]

    # Group records by country
    rec = sorted(records, key=countryCode)

    totalsum = {dataprocess.HOST: "ALL-TOTAL"}
    countrysum = {}
    n = 0
    i = 0
    while i < len(rec):
        country = countryCode(rec[i]).upper() + "-TOTAL"

        if dataprocess.HOST not in countrysum:
            countrysum[dataprocess.HOST] = country

        if countrysum[dataprocess.HOST] != country:
            if n > 0: # '> 1' if we want sums only for multi-cluster countries
                countrysum[dataprocess.EFFICIENCY] = int(100.0*countrysum[dataprocess.HS06_CPU_TIME]/countrysum[dataprocess.HS06_WALL_TIME])
                rec.insert(i, countrysum)
                i += 1
            countrysum = {dataprocess.HOST: country}
            n = 0

        for key in rec[i]:
            if key not in (dataprocess.HOST, dataprocess.TIER, dataprocess.EFFICIENCY):
                val = rec[i][key]
                countrysum[key] = countrysum.get(key, 0) + val
                totalsum[key] = totalsum.get(key, 0) + val
        n += 1
        i += 1


    countrysum[dataprocess.EFFICIENCY] = int(100.0*countrysum[dataprocess.HS06_CPU_TIME]/countrysum[dataprocess.HS06_WALL_TIME])
    rec.append(countrysum)
    totalsum[dataprocess.EFFICIENCY] = int(100.0*totalsum[dataprocess.HS06_CPU_TIME]/totalsum[dataprocess.HS06_WALL_TIME])
    rec.append(totalsum)
    return rec

class WLCGVOOversightView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.VO_ROLE, dataprocess.USER)
    columns = [ dataprocess.HOST,
                dataprocess.N_JOBS, dataprocess.HS06_WALL_TIME, dataprocess.HS06_CPU_TIME, dataprocess.HS06_CPU_EQUIVALENTS, dataprocess.EFFICIENCY ]
    tier_based = True
    split = dataprocess.VO_NAME
    sort = staticmethod(sortAndSumByCountry)


class WLCGMachineView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.VO_ROLE, dataprocess.USER )
    columns = [ dataprocess.HOST, dataprocess.VO_NAME, dataprocess.N_JOBS,
                dataprocess.WALL_TIME, dataprocess.WALL_EQUIVALENTS, dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]

class WLCGMachinePerMonthView(WLCGBaseView):

    collapse = ( dataprocess.VO_GROUP, dataprocess.VO_ROLE, dataprocess.USER )
    columns = [ dataprocess.YEAR, dataprocess.MONTH, dataprocess.HOST, dataprocess.VO_NAME, dataprocess.N_JOBS,
                dataprocess.WALL_TIME, dataprocess.CPU_TIME, dataprocess.EFFICIENCY ]

class WLCGUserView(WLCGBaseView):

    collapse = ( dataprocess.YEAR, dataprocess.MONTH, dataprocess.HOST, dataprocess.VO_GROUP )
    columns = [ dataprocess.USER, dataprocess.VO_NAME, dataprocess.VO_ROLE, dataprocess.N_JOBS,
                dataprocess.KSI2K_WALL_TIME, dataprocess.KSI2K_WALL_EQUIVALENTS, dataprocess.EFFICIENCY ]
    viewgroup = 'restricted'



WLCG_UNIT_MAPPING_DEFAULT = lambda rec : rec[dataprocess.KSI2K_WALL_EQUIVALENTS]
WLCG_UNIT_MAPPING = {
    'ksi2k-ne' : WLCG_UNIT_MAPPING_DEFAULT,
    'hs06-ne'  : lambda rec : rec[dataprocess.HS06_WALL_EQUIVALENTS],
    'hs06-cpune'  : lambda rec : rec[dataprocess.HS06_CPU_EQUIVALENTS],
    'ksi2k-wallhours' : lambda rec : rec[dataprocess.KSI2K_WALL_TIME],
    'hs06-wallhours'  : lambda rec : rec[dataprocess.HS06_WALL_TIME],
    'hs06-cpuhours'  : lambda rec : rec[dataprocess.HS06_CPU_TIME]
}



class WLCGOversightView(baseview.BaseView):

    # This view is rather different than the others, so it is its own class
    collapse = [ dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER ]

    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)

        wlcg_config = json.load(open(mfst.getProperty('wlcg_config_file')))
        self.tier_mapping = wlcg_config['tier-mapping']
        self.tier_shares  = wlcg_config['tier-ratio']
        self.default_tier = wlcg_config['default-tier']
        self.hepspec06  = wlcg_config['hepspec06']


    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg'), (rights.CTX_VIEWGROUP, 'pub') ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG oversight view', subject)

        # access allowed
        start_date, end_date = dateform.parseStartEndDates(request)

        # set dates if not specified (defaults are current month, which is not what we want)
        year, quart = dateform.currentYearQuart()
        if not 'startdate' in request.args or request.args['startdate'] == ['']:
            start_date, _ = dateform.quarterStartEndDates(year, quart)
        if not 'enddate' in request.args or request.args['enddate'] == ['']:
            _, end_date = dateform.quarterStartEndDates(year, quart)
        if 'unit' in request.args and request.args['unit'][0] not in WLCG_UNIT_MAPPING:
            return self.renderErrorPage('Invalid units parameters')
        unit = request.args.get('unit', ['ksi2k-ne'])[0]

        t_query_start = time.time()
        d = self.retrieveWLCGData(start_date, end_date)
        d.addCallback(self.renderWLCGViewPage, request, start_date, end_date, unit, t_query_start)
        d.addErrback(self.renderErrorPage, request)
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, start_date, end_date):

        d = self.urdb.query(wlcgquery.WLCG_QUERY, (start_date, end_date))
        return d


    def renderWLCGViewPage(self, wlcg_data, request, start_date, end_date, unit, t_query_start):

        t_query = time.time() - t_query_start
        days = dateform.dayDelta(start_date, end_date)
        t_dataprocess_start = time.time()

        wlcg_records = dataprocess.rowsToDicts(wlcg_data)
        # information on ops and dteam vo does not add any value
        wlcg_records = [ rec for rec in wlcg_records if rec[dataprocess.VO_NAME] not in ('dteam', 'ops') ]

        # massage data
        wlcg_records = dataprocess.addMissingScaleValues(wlcg_records, self.hepspec06)
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
        assert len(total) in (0,1), 'Records did not collapse into a single record when calculating grand total'
        if len(total) == 0:
            total = [ { dataprocess.CPU_TIME : 0, dataprocess.WALL_TIME : 0, dataprocess.KSI2K_CPU_TIME : 0, dataprocess.KSI2K_WALL_TIME : 0 } ]
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

        unit_extractor = WLCG_UNIT_MAPPING.get(unit, WLCG_UNIT_MAPPING_DEFAULT)

        elements = []
        for row in row_names:
            for col in columns:
                for rec in wlcg_records:
                    if rec[dataprocess.HOST] == row and rec[dataprocess.VO_NAME] == col:
                        value = _formatValue( unit_extractor(rec) )
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
        start_date_option = request.args.get('startdate', [''])[0]
        end_date_option   = request.args.get('enddate', [''])[0]

        title = 'WLCG oversight view'
        unit_options = [ ( 'hs06-cpuhours', 'HS06 CPUtime Hours' ), ( 'hs06-cpune', 'HS06 CPU node Equivalents' ),
                         ( 'hs06-wallhours', 'HS06 Walltime hours'), ( 'hs06-ne', 'HS06 Wall Node Equivalents' )]
        unit_buttons = html.createRadioButtons('unit', unit_options, checked_value=unit)
        selector_form = dateform.createMonthSelectorForm(self.path, start_date_option, end_date_option, unit_buttons)

        quarters = dateform.generateFormQuarters()
        quarter_links = []
        for q in quarters:
            year, quart = dateform.parseQuarter(q)
            sd, ed = dateform.quarterStartEndDates(year, quart)
            quarter_links.append(html.createLink('%s?startdate=%s&enddate=%s' % (self.path, sd, ed), q ) )
        range_text = html.createParagraph('Date range: %s - %s (%s days)' % (start_date, end_date, days))

        request.write( html.HTML_VIEWBASE_HEADER % {'title': title} )
        request.write( html.createTitle(title) )
        request.write( html.createParagraph('Quarters: \n    ' + ('    ' + html.NBSP).join(quarter_links) ) )
        request.write( html.SECTION_BREAK )
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



class WLCGT1SummaryView(baseview.BaseView):
    # This view is rather different than the others, so it is its own class

    collapse = [ dataprocess.YEAR, dataprocess.MONTH, dataprocess.VO_GROUP, dataprocess.USER ]

    # Make a storage query that can be UNIONized with a WLCG_QUERY; Storage
    # number will be stored in 'n_jobs'
    storage_query = """
    SELECT extract(YEAR FROM end_time)::integer  AS year,
           extract(MONTH FROM end_time)::integer AS month,
           'STORAGE' as machine_name,
           CASE WHEN group_identity LIKE 'atlas-%%' THEN
               'atlas'
           ELSE 
               group_identity 
           END AS vo_name,
           storage_media AS vo_group,
           '' AS vo_role,
           '' AS user_identity,
           sum(resource_capacity_used) AS n_jobs,
           0 AS cputime,
           0 AS walltime,
           0 AS cputime_scaled,
           0 AS walltime_scaled
     FROM storagerecords
     WHERE end_time = (SELECT max(end_time) FROM storagerecords WHERE end_time >= %s AND end_time <= %s) AND
           group_identity NOT IN ('atlas-no', 'atlas-dk')
     GROUP BY end_time, storage_media, vo_name"""


    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)

        wlcg_config = json.load(open(mfst.getProperty('wlcg_config_file')))
        self.tier_mapping = wlcg_config['tier-mapping']
        self.tier_shares  = wlcg_config['tier-ratio']
        self.hepspec06  = wlcg_config['hepspec06']
        self.default_tier = wlcg_config['default-tier']
        
        cfg = config.readConfig("/etc/sgas.conf") 
        self.db_url = cfg.get(config.SERVER_BLOCK, config.DB)



    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg'), (rights.CTX_VIEWGROUP, 'pub') ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG T1 Summary', subject)

        # access allowed
        start_date, end_date = dateform.parseStartEndDates(request)

        # set dates if not specified (defaults are current month, which is not what we want)
        year, quart = dateform.currentYearQuart()
        if not 'startdate' in request.args or request.args['startdate'] == ['']:
            start_date, _ = dateform.quarterStartEndDates(year, quart)
        if not 'enddate' in request.args or request.args['enddate'] == ['']:
            _, end_date = dateform.quarterStartEndDates(year, quart)
        if 'unit' in request.args and request.args['unit'][0] not in WLCG_UNIT_MAPPING:
            return self.renderErrorPage('Invalid units parameters')
        unit = request.args.get('unit', ['hs06-wallhours'])[0]

        t_query_start = time.time()
        d = self.retrieveWLCGData(start_date, end_date)
        d.addCallback(self.renderWLCGViewPage, request, start_date, end_date, unit, t_query_start)
        d.addErrback(self.renderErrorPage, request)
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, start_date, end_date):

        query = self.storage_query + ' UNION ' + wlcgquery.WLCG_QUERY

        d = self.urdb.query(query, (start_date, end_date, start_date, end_date))
        return d


    def renderWLCGViewPage(self, wlcg_data, request, start_date, end_date, unit, t_query_start):

        t_query = time.time() - t_query_start
        days = dateform.dayDelta(start_date, end_date)
        t_dataprocess_start = time.time()

        wlcg_records = dataprocess.rowsToDicts(wlcg_data)
        wlcg_records = [ r for r in wlcg_records if r[dataprocess.VO_NAME] in ('alice', 'atlas') ]

        # Separate the records into computing and storage
        comp_records = []
        storage_records = []
        for r in wlcg_records:
            if r[dataprocess.HOST] == 'STORAGE':
                storage_records.append(r)
            else:
                comp_records.append(r)

        # massage data
        comp_records = dataprocess.addMissingScaleValues(comp_records, self.hepspec06)
        comp_records = dataprocess.collapseFields(comp_records, self.collapse)
        comp_records = dataprocess.tierMergeSplit(comp_records, self.tier_mapping, self.tier_shares, self.default_tier)

        # Collapse the fields that couldn't be collapsed before the tier-splitting.
        comp_records = [ r for r in comp_records if r['tier'] == u'NDGF-T1' ]
        comp_records = dataprocess.collapseFields(comp_records, [ dataprocess.VO_ROLE, dataprocess.HOST ])


        summary = {}
        for r in comp_records + storage_records:
            vo = r[dataprocess.VO_NAME] 

            if vo not in summary:
                summary[vo] = {'disk':0.0, 'tape':0.0, dataprocess.HS06_WALL_TIME:0.0, dataprocess.HS06_CPU_TIME:0.0}
            
            if r.get(dataprocess.HOST, '') == 'STORAGE':
                media = r['vo_group']                        # We stored "storage_media" in "vo_group" ...
                summary[vo][media] += r[dataprocess.N_JOBS]  # ... and "resource_capacity_used" in "n_jobs". 
            else:
                summary[vo][dataprocess.HS06_WALL_TIME] += r[dataprocess.HS06_WALL_TIME]
                summary[vo][dataprocess.HS06_CPU_TIME] += r[dataprocess.HS06_CPU_TIME]

        columns = [("Walltime (HS06 days)", lambda r: r[dataprocess.HS06_WALL_TIME] / 24.0),
                   ("CPUtime (HS06 days)",  lambda r: r[dataprocess.HS06_CPU_TIME] / 24.0),
                   ("Disk (TiB)",           lambda r: r['disk'] / 1024.0**4),
                   ("Tape (TiB)",           lambda r: r['tape'] / 1024.0**4)]

        elements = []
        for row in summary.keys():
            for cname,cfunc in columns:
                value = "%.2f" % cfunc(summary[row])
                elements.append( ((cname,row), value))

        t_dataprocess = time.time() - t_dataprocess_start

        matrix = dict(elements)
        table_content = htmltable.createHTMLTable(matrix, [c[0] for c in columns], summary.keys())

        # render page
        start_date_option = request.args.get('startdate', [''])[0]
        end_date_option   = request.args.get('enddate', [''])[0]

        title = 'WLCG T1 Summary'
        selector_form = dateform.createMonthSelectorForm(self.path, start_date_option, end_date_option)

        quarters = dateform.generateFormQuarters()
        quarter_links = []
        for q in quarters:
            year, quart = dateform.parseQuarter(q)
            sd, ed = dateform.quarterStartEndDates(year, quart)
            quarter_links.append(html.createLink('%s?startdate=%s&enddate=%s' % (self.path, sd, ed), q ) )
        range_text = html.createParagraph('Date range: %s - %s (%s days)' % (start_date, end_date, days))

        request.write( html.HTML_VIEWBASE_HEADER % {'title': title} )
        request.write( html.createTitle(title) )
        request.write( html.createParagraph('Quarters: \n    ' + ('    ' + html.NBSP).join(quarter_links) ) )
        request.write( html.SECTION_BREAK )
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



## STORAGE STUFF



class WLCGStorageView(baseview.BaseView):

    WLCG_STORAGE_QUERY = """
        SELECT storage_share, group_identity, storage_media, (sum(r) / 1099511627776)::integer FROM (
            SELECT
                DISTINCT ON (t.sample, storage_system, ss.storage_share, storage_media, storage_class, sg.group_identity)
                ss.storage_share, sg.group_identity, storage_media, COALESCE(resource_capacity_used, 0) as r
            FROM
                (SELECT %(timestamp)s::timestamp  AS sample) AS t
                CROSS JOIN (SELECT DISTINCT storage_share  FROM storagerecords WHERE start_time <= %(timestamp)s AND end_time >= %(timestamp)s) AS ss
                CROSS JOIN (SELECT DISTINCT group_identity FROM storagerecords WHERE storage_system = 'dcache.ndgf.org' AND start_time <= %(timestamp)s AND end_time >= %(timestamp)s) AS sg
                LEFT OUTER JOIN storagerecords ON (start_time <= t.sample AND end_time >= t.sample AND
                                                   ss.storage_share = storagerecords.storage_share AND
                                                   sg.group_identity = storagerecords.group_identity)
            WHERE resource_capacity_used IS NOT NULL
            ORDER BY t.sample, storage_system, ss.storage_share, storage_media, storage_class, sg.group_identity, end_time DESC) as s
        GROUP BY s.storage_share, s.group_identity, storage_media
        ORDER BY s.storage_share, s.group_identity, storage_media;
    """

    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)

        wlcg_config = json.load(open(mfst.getProperty('wlcg_config_file')))
        self.default_tier = wlcg_config['default-tier']

    def render_GET(self, request):
        subject = resourceutil.getSubject(request)

        # authz check
        ctx = [ (rights.CTX_VIEWGROUP, 'wlcg'), (rights.CTX_VIEWGROUP, 'pub') ]
        if not self.authorizer.isAllowed(subject, rights.ACTION_VIEW, ctx):
            return self.renderAuthzErrorPage(request, 'WLCG Storage View', subject)

        # access allowed
        date = dateform.parseDate(request)
        media = request.args.get('media',['all'])[0]

        t_query_start = time.time()
        d = self.retrieveWLCGData(date)
        d.addCallback(self.renderWLCGViewPage, request, date, media, t_query_start)
        d.addErrback(self.renderErrorPage, request)
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, date):

        d = self.urdb.query(self.WLCG_STORAGE_QUERY, {'timestamp': date } )
        return d


    def buildRecords(self, db_rows):

        records = []
        for row in db_rows:
            site, group, media, rcu = row
            site = site.replace('_', '.')
            records.append( {'site':site, 'group':group, 'media':media, 'rcu': rcu } )
        return records


    def renderWLCGViewPage(self, db_rows, request, date, media, t_query_start):

        t_query = time.time() - t_query_start
        t_dataprocess_start = time.time()

        records = self.buildRecords(db_rows)

        # remove infomration from certain groups, add they not add any value
        records = [ rec for rec in records if rec['group'] not in ('dteam', 'behrmann') ]
        if media == 'disk':
            records = [ rec for rec in records if rec['media'] == 'disk' ]
        elif media == 'tape':
            records = [ rec for rec in records if rec['media'] == 'tape' ]
        elif media == 'all':
            pass
        else:
            return self.renderErrorPage('Invalid media selection', request)

        # get top level domains and sites per country
        hosts = set( [ rec['site'] for rec in records ] )
        tld_groups = {}
        country_sites = {}
        for host in hosts:
            tld = host.split('.')[-1].upper()
            tld_groups.setdefault(tld, []).append(host)

        # get groups
        groups = set( [ rec['group'] for rec in records ] )

        TOTAL = 'Total'
        TIER_TOTAL = self.default_tier.split('-')[0].upper()

        # calculate totals per site / group
        site_group_totals = {}
        for rec in records:
            site, group, rcu = rec['site'], rec['group'], rec['rcu']
            key = (site, group)
            site_group_totals[key] = site_group_totals.get(key, 0) + rcu

        # calculate total per site
        site_totals = {}
        for rec in records:
            site, rcu = rec['site'], rec['rcu']
            site_totals[site] = site_totals.get(site,0) + rcu

        # calculate total per group / country
        group_country_totals = {}
        for rec in records:
            country = rec['site'].split('.')[-1].upper() + '-TOTAL'
            key = (country, rec['group'])
            group_country_totals[key] = group_country_totals.get(key, 0) + rec['rcu']

        # calculate total per country
        country_totals = {}
        for rec in records:
            country = rec['site'].split('.')[-1].upper() + '-TOTAL'
            country_totals[country] = country_totals.get(country,0) + rec['rcu']

        # calculate total per group
        group_totals = {}
        for rec in records:
            group_totals[rec['group']] = group_totals.get(rec['group'],0) + rec['rcu']

        # calculate total
        total = sum( [ rec['rcu'] for rec in records ] )

        totals = []

        # put all calculated records together and add equivalents
        for (site, group), rcu in site_group_totals.items():
            totals.append( { 'site': site, 'group': group, 'rcu': rcu } )
        for site, rcu in site_totals.items():
            totals.append( { 'site': site, 'group': TOTAL, 'rcu': rcu } )
        for (country, group), rcu in group_country_totals.items():
            totals.append( { 'site': country, 'group': group, 'rcu': rcu } )
        for country, rcu in country_totals.items():
            totals.append( { 'site': country, 'group': TOTAL, 'rcu': rcu } )
        for group, rcu in group_totals.items():
            totals.append( { 'site': TIER_TOTAL, 'group': group, 'rcu': rcu } )
        totals.append( { 'site': TIER_TOTAL, 'group': TOTAL, 'rcu': total } )

        # create table
        columns = sorted(groups)
        columns.append(TOTAL)

        row_names = []
        for tld in sorted(tld_groups):
            row_names += sorted(tld_groups[tld])
            row_names.append(tld + '-TOTAL')
        row_names.append(TIER_TOTAL)

        elements = []
        for row in row_names:
            for col in columns:
                for rec in totals:
                    if rec['site'] == row and rec['group'] == col:
                        value = rec['rcu']
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
        t_dataprocess = time.time() - t_dataprocess_start
        table_content = htmltable.createHTMLTable(matrix, columns, row_names, column_labels=COLUMN_NAMES)

        title = 'WLCG storage view'
        media_options = [ ( 'all', 'Disk and Tape') , ( 'disk', 'Disk only' ), ( 'tape', 'Tape only' ) ]

        media_buttons = html.createRadioButtons('media', media_options, checked_value=media)
        month_options = dateform.generateMonthFormOptions()
        selector = html.createSelector('Month', 'date', month_options, date)
        selector_form = html.createSelectorForm(self.path, [ selector ], media_buttons )

        date_text = html.createParagraph('Date: %s' % (date))

        request.write( html.HTML_VIEWBASE_HEADER % {'title': title} )
        request.write( html.createTitle(title) )
        request.write( html.createParagraph(selector_form) )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph(date_text) )
        request.write( table_content )
        request.write( html.SECTION_BREAK )
        request.write( html.createParagraph('Query time: %s' % round(t_query, 2)) )
        request.write( html.createParagraph('Data process time: %s' % round(t_dataprocess, 2)) )
        request.write( html.HTML_VIEWBASE_FOOTER )

        request.finish()
        return server.NOT_DONE_YET


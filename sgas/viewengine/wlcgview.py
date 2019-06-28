"""
WLCG View. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
        Erik Edelmann <edelmann@csc.fi>
Copyright: Nordic Data Grid Facility (2011), Nordic e-Infrastructure Collaboration (2016-2019)
"""

import time
import json

from sgas.wlcg import wlcg

from twisted.web import server

from sgas.server import resourceutil, config
from sgas.viewengine import html, htmltable, dateform, baseview, rights

CPU_DAYS_HS06 = 'cpu_days_hs06'
CORE_DAYS_HS06 = 'core_days_hs06'
DISK_TIB = 'disk_tib'
TAPE_TIB = 'tape_tib'

# Mapping for more readable column names
COLUMN_NAMES = {
    wlcg.YEAR                    : 'Year',
    wlcg.MONTH                   : 'Month',
    wlcg.TIER                    : 'Tier',
    wlcg.MACHINE_NAME            : 'Host',
    wlcg.VO_NAME                 : 'VO',
    wlcg.VO_GROUP                : 'VO Group',
    wlcg.VO_ROLE                 : 'VO Role',
    wlcg.USER                    : 'User',
    wlcg.N_JOBS                  : 'Job count',
    wlcg.CPU_SECONDS             : 'CPU hours',   # (We change time units from s to h)
    wlcg.CORE_SECONDS            : 'Wall hours',  # Yeah, I know ... but s omebody decided calling it "Wall", so we'll keep it
    wlcg.CPU_SECONDS_HS06        : 'HS06 CPU hours',
    wlcg.CORE_SECONDS_HS06       : 'HS06 wall hours',
    wlcg.EFFICIENCY              : 'Job efficiency',
    #wlcg.CPU_EQUIVALENTS        : 'CPU node equivalents',
    wlcg.CORE_EQUIVALENTS        : 'Wall node equivalents',
    wlcg.HS06_CPU_EQUIVALENTS    : 'HS06 CPU node equivalents',
    wlcg.HS06_CORE_EQUIVALENTS   : 'HS06 Wall node equivalents',
    CPU_DAYS_HS06                : 'CPU days (HS06)',
    CORE_DAYS_HS06               : 'Wall days (HS06)',
    DISK_TIB                     : 'Disk (TiB)',
    TAPE_TIB                     : 'Tape (TiB)'
}

def _formatValue(v):
    if type(v) is float:
        return int(v)
    else:
        return v


def _sortKey(record, field_order):
    """
    Given a record, returns a key usable for sorting.
    """
    attrs = []
    for f in field_order:
        if f in record:
            attrs.append(record[f])
    return tuple(attrs)


def _changeUnits(records):
    """
    Efficiency -> %
    Seconds -> hours
    """

    # Columns that has times in seconds, that needs conversion to hours
    time_columns = (wlcg.CPU_SECONDS, wlcg.CORE_SECONDS, wlcg.CPU_SECONDS_HS06, wlcg.CORE_SECONDS_HS06)

    for r in records:
        if wlcg.EFFICIENCY in r:
            if r[wlcg.EFFICIENCY]:
                r[wlcg.EFFICIENCY] *= 100
            else:
                r[wlcg.EFFICIENCY] = '-'

        for c in time_columns:
            if c in r:
                r[c] /= 3600

    return records


def _splitRecords(records, split_attribute):

    split_records = {}

    for rec in records:

        rc = rec.copy()
        split_records.setdefault(rc[split_attribute], []).append(rc)
        del rc[split_attribute]

    return split_records

def _mergeRecords(records):
    """
    Merge two or more records, keeping their base information, but adding count variables.
    Assumes base information is identical.
    """
    def sumfield(dicts, field):
        fields = [ d[field] for d in dicts ]
        result = sum(fields) if not None in fields else None
        return result

    nr = records[0].copy()
    for c in wlcg.value_columns:
        if c in nr:
            nr[c] = sumfield(records, c)

    return nr


def _collapseFields(records, collapse_fields):
    """
    Removes one or more key fields in the records and sums togther the records
    into a new batch of records for the new shared key fields.
    """

    KEY_FIELDS = ( wlcg.YEAR, wlcg.MONTH, wlcg.TIER, wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.VO_GROUP, wlcg.VO_ROLE, wlcg.USER )

    def createFieldKey(record):
        """
        Returns the key fields of a record as a tuple.
        """
        key = tuple ( [ record[field] for field in KEY_FIELDS if field in record ] )
        return key


    collapsed_records = {}

    for rec in records:
        r = rec.copy()
        for cf in collapse_fields:
            del r[cf]
        key = createFieldKey(r)
        collapsed_records.setdefault(key, []).append(r)

    summed_records = []
    for records in collapsed_records.values():
        summed_records.append( _mergeRecords(records) )

    return summed_records



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

    group_by = []
    columns = []
    vo_list = ('atlas', 'alice', 'cms')
    split = None
    tier_based = False
    viewgroup = 'pub'
    sort = staticmethod(sorted)

    def __init__(self, urdb, authorizer, mfst, path):
        self.path = path
        baseview.BaseView.__init__(self, urdb, authorizer, mfst)
        self.wlcgdb = wlcg.WLCG(urdb)


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

        end_date += " 23:59:59"
        self.wlcgdb.add_query(columns=self.columns,group_by=self.group_by,timerange=(start_date,end_date),vo_list=self.vo_list)
        d = self.wlcgdb.fetch()
        return d


    def renderWLCGViewPage(self, wlcg_data, request, start_date, end_date, t_query_start):

        t_query = time.time() - t_query_start

        days = dateform.dayDelta(start_date, end_date)

        t_dataprocess_start = time.time()

        wlcg_records = wlcg.rowsToDicts(wlcg_data, self.columns)
        wlcg_records = _changeUnits(wlcg_records)

        sk = lambda key : _sortKey(key, field_order=self.columns)
        if self.split is None:
            wlcg_records = self.sort(wlcg_records, key=sk)
        else:
            split_records = _splitRecords(wlcg_records, self.split)
            for split_attr, records in split_records.items():
                split_records[split_attr] = self.sort(records, key=sk)

        t_dataprocess = time.time() - t_dataprocess_start

        if self.split is None:
            table_content = self.createTable(wlcg_records, self.columns)
        else:
            table_content = ''
            columns = [ c for c in self.columns if c != self.split ]
            for split_attr, records in split_records.items():
                table = self.createTable(records, columns)
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

    group_by = ( wlcg.VO_NAME, wlcg.VO_ROLE, wlcg.MACHINE_NAME )
    columns = [ wlcg.VO_NAME, wlcg.VO_ROLE, wlcg.MACHINE_NAME, 
                wlcg.N_JOBS, wlcg.CORE_SECONDS, wlcg.CORE_EQUIVALENTS,
                wlcg.CORE_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]


class WLCGTierView(WLCGBaseView):

    group_by = ( wlcg.TIER, wlcg.VO_NAME, wlcg.VO_ROLE )
    columns = [ wlcg.TIER, wlcg.VO_NAME, wlcg.VO_ROLE,
                wlcg.N_JOBS, wlcg.CORE_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]
    tier_based = True



class WLCGFullTierView(WLCGBaseView):

    group_by = ( wlcg.TIER, wlcg.VO_NAME, wlcg.VO_GROUP, wlcg.VO_ROLE, wlcg.USER )
    columns = [ wlcg.TIER, wlcg.VO_NAME, wlcg.VO_GROUP, wlcg.VO_ROLE, wlcg.USER,
                wlcg.N_JOBS, wlcg.CORE_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]
    tier_based = True
    viewgroup = 'restricted'



class WLCGTierMachineSplitView(WLCGBaseView):

    group_by = ( wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.VO_ROLE, wlcg.TIER )
    columns = [ wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.VO_ROLE, wlcg.TIER,
                wlcg.N_JOBS, wlcg.CORE_SECONDS_HS06, wlcg.CPU_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]
    tier_based = True
    split = wlcg.TIER


def sortAndSumByCountry(records, key):
    """
    Sort the records by country, and insert records representing sum over
    the countries, and append a total sum at the end.

    The argument 'key' is there for compatibility with the builtin function
    'sorted' only.

    """

    countryCode = lambda x: x[wlcg.MACHINE_NAME].split(".")[-1]

    # Group records by country
    rec = sorted(records, key=countryCode)

    totalsum = {wlcg.MACHINE_NAME: "ALL-TOTAL"}
    countrysum = {}
    n = 0
    i = 0
    while i < len(rec):
        country = countryCode(rec[i]).upper() + "-TOTAL"

        if wlcg.MACHINE_NAME not in countrysum:
            countrysum[wlcg.MACHINE_NAME] = country

        if countrysum[wlcg.MACHINE_NAME] != country:
            if n > 0: # '> 1' if we want sums only for multi-cluster countries
                countrysum[wlcg.EFFICIENCY] = int(100.0*countrysum[wlcg.CPU_SECONDS_HS06]/countrysum[wlcg.CORE_SECONDS_HS06])
                rec.insert(i, countrysum)
                i += 1
            countrysum = {wlcg.MACHINE_NAME: country}
            n = 0

        for key in rec[i]:
            if key not in (wlcg.MACHINE_NAME, wlcg.TIER, wlcg.EFFICIENCY, wlcg.VO_NAME):
                val = rec[i][key]
                countrysum[key] = countrysum.get(key, 0) + val
                totalsum[key] = totalsum.get(key, 0) + val
        n += 1
        i += 1


    countrysum[wlcg.EFFICIENCY] = int(100.0*countrysum[wlcg.CPU_SECONDS_HS06]/countrysum[wlcg.CORE_SECONDS_HS06])
    rec.append(countrysum)
    totalsum[wlcg.EFFICIENCY] = int(100.0*totalsum[wlcg.CPU_SECONDS_HS06]/totalsum[wlcg.CORE_SECONDS_HS06])
    rec.append(totalsum)
    return rec

class WLCGVOOversightView(WLCGBaseView):

    group_by = (  wlcg.MACHINE_NAME, wlcg.VO_NAME )
    columns = [ wlcg.MACHINE_NAME, wlcg.VO_NAME,
                wlcg.N_JOBS, wlcg.CORE_SECONDS_HS06, wlcg.CPU_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]
    tier_based = True
    split = wlcg.VO_NAME
    sort = staticmethod(sortAndSumByCountry)


class WLCGMachineView(WLCGBaseView):

    columns = [ wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.N_JOBS,
                wlcg.CORE_SECONDS, wlcg.CORE_EQUIVALENTS, wlcg.CORE_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]
    group_by = ( wlcg.MACHINE_NAME, wlcg.VO_NAME )

class WLCGMachinePerMonthView(WLCGBaseView):

    group_by = ( wlcg.YEAR, wlcg.MONTH, wlcg.MACHINE_NAME, wlcg.VO_NAME )
    columns = [ wlcg.YEAR, wlcg.MONTH, wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.N_JOBS,
                wlcg.CORE_SECONDS, wlcg.CPU_SECONDS, wlcg.EFFICIENCY ]

class WLCGUserView(WLCGBaseView):

    group_by = ( wlcg.USER, wlcg.VO_NAME, wlcg.VO_ROLE )
    columns = [ wlcg.USER, wlcg.VO_NAME, wlcg.VO_ROLE, wlcg.N_JOBS,
                wlcg.CORE_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS, wlcg.EFFICIENCY ]
    viewgroup = 'restricted'


"""
WLCG_UNIT_MAPPING_DEFAULT = lambda rec : rec[wlcg.HS06_CORE_EQUIVALENTS]
WLCG_UNIT_MAPPING = {
    'ksi2k-ne' : WLCG_UNIT_MAPPING_DEFAULT,
    'hs06-ne'  : lambda rec : rec[wlcg.HS06_CORE_EQUIVALENTS],
    'hs06-cpune'  : lambda rec : rec[wlcg.HS06_CPU_EQUIVALENTS],
    'ksi2k-wallhours' : lambda rec : rec[wlcg.WALL_SECONDS_HS06],
    'hs06-wallhours'  : lambda rec : rec[wlcg.WALL_SECONDS_HS06],
    'hs06-cpuhours'  : lambda rec : rec[wlcg.CPU_SECONDS_HS06]
}
"""



class WLCGOversightView(WLCGBaseView):

    # This view is rather different than the others, so it is its own class
    group_by = [ wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.TIER ]
    units = (wlcg.CPU_SECONDS_HS06,  wlcg.HS06_CPU_EQUIVALENTS, wlcg.CORE_SECONDS_HS06, wlcg.HS06_CORE_EQUIVALENTS)
    default_tier = 'ndgf-t1'

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
        if 'unit' in request.args and request.args['unit'][0] not in self.units:
            return self.renderErrorPage('Invalid units parameters')
        unit = request.args.get('unit', [wlcg.HS06_CORE_EQUIVALENTS])[0]

        t_query_start = time.time()
        d = self.retrieveWLCGData(start_date, end_date, unit)
        d.addCallback(self.renderWLCGViewPage, request, start_date, end_date, unit, t_query_start)
        d.addErrback(self.renderErrorPage, request)
        return server.NOT_DONE_YET


    def retrieveWLCGData(self, start_date, end_date, unit):

        end_date += " 23:59:59"
        columns = [ wlcg.MACHINE_NAME, wlcg.VO_NAME, wlcg.TIER, unit ]
        self.wlcgdb.add_query(columns=columns, group_by=self.group_by, timerange=(start_date,end_date), vo_list=self.vo_list)

        columns = (
            {'name': wlcg.MACHINE_NAME, 'code': wlcg.MACHINE_NAME},
            {'name': wlcg.VO_NAME, 'code': "case when %s like '%%%%-t1' then 't1-' || %s else 't2-' || %s end" % (wlcg.TIER, wlcg.VO_NAME, wlcg.VO_NAME)},
            {'name': unit, 'code': 'sum(' + unit + ')'}
        )
        self.wlcgdb.add_outer_query(columns=columns, group_by=(wlcg.VO_NAME, wlcg.MACHINE_NAME, wlcg.TIER))
        d = self.wlcgdb.fetch()
        return d


    def renderWLCGViewPage(self, wlcg_data, request, start_date, end_date, unit, t_query_start):

        t_query = time.time() - t_query_start
        days = dateform.dayDelta(start_date, end_date)
        t_dataprocess_start = time.time()

        wlcg_records = wlcg.rowsToDicts(wlcg_data, [ wlcg.MACHINE_NAME, wlcg.VO_NAME, unit ])

        t_dataprocess = time.time() - t_dataprocess_start

        # get tld groups
        hosts = set( [ rec[wlcg.MACHINE_NAME] for rec in wlcg_records ] )
        tld_groups = {}
        for host in hosts:
            tld = host.split('.')[-1].upper()
            tld_groups.setdefault(tld, []).append(host)

        vo_tiers = set()
        for rec in wlcg_records:
            vo_tiers.add(rec[wlcg.VO_NAME])

        TOTAL = 'Total'
        TIER_TOTAL = self.default_tier.split('-')[0].upper()

        site_totals = _collapseFields(wlcg_records, ( wlcg.VO_NAME, ) )
        for r in site_totals:
            r[wlcg.VO_NAME] = TOTAL

        # calculate total per country-tier
        country_tier_totals = [ r.copy() for r in wlcg_records ]
        for rec in country_tier_totals:
            rec[wlcg.MACHINE_NAME] = rec[wlcg.MACHINE_NAME].split('.')[-1].upper() + '-TOTAL'

        # calculate total per country
        country_totals = _collapseFields(country_tier_totals, ( wlcg.VO_NAME, ) )
        for rec in country_totals:
            rec[wlcg.VO_NAME] = TOTAL

        # calculate total per tier-vo
        tier_vo_totals = _collapseFields(wlcg_records, ( wlcg.MACHINE_NAME, ) )
        for r in tier_vo_totals:
            r[wlcg.MACHINE_NAME] = TIER_TOTAL

        # calculate total
        total = _collapseFields(wlcg_records, ( wlcg.MACHINE_NAME, wlcg.VO_NAME ) )
        assert len(total) in (0,1), 'Records did not collapse into a single record when calculating grand total'
        if len(total) == 0:
            total = [ { wlcg.CPU_SECONDS : 0, wlcg.CORE_SECONDS : 0, wlcg.CPU_SECONDS_HS06 : 0, wlcg.CORE_SECONDS_HS06 : 0 } ]
        total_record = total[0]
        total_record[wlcg.MACHINE_NAME] = TIER_TOTAL
        total_record[wlcg.VO_NAME] = TOTAL

        # put all calculated records together and add equivalents
        wlcg_records += site_totals
        wlcg_records += country_tier_totals
        wlcg_records += country_totals
        wlcg_records += tier_vo_totals
        wlcg_records += [ total_record ]

        # create table
        columns = sorted(vo_tiers)
        columns.append(TOTAL)

        row_names = []
        for tld in sorted(tld_groups):
            row_names += tld_groups[tld]
            row_names.append(tld + '-TOTAL')
        row_names.append(TIER_TOTAL)

        #unit_extractor = WLCG_UNIT_MAPPING.get(unit, WLCG_UNIT_MAPPING_DEFAULT)
        unit_extractor = lambda rec : rec[unit]

        elements = []
        for row in row_names:
            for col in columns:
                for rec in wlcg_records:
                    if rec[wlcg.MACHINE_NAME] == row and rec[wlcg.VO_NAME] == col:
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
        unit_options = []
        for u in self.units:
            unit_options.append(( u, COLUMN_NAMES[u] ))
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



class WLCGT1SummaryView(WLCGBaseView):
    # This view is rather different than the others, so it is its own class

    columns = (wlcg.VO_NAME, CORE_DAYS_HS06, CPU_DAYS_HS06, DISK_TIB, TAPE_TIB)
    group_by = (wlcg.VO_NAME)
    vo_list = ('atlas', 'alice')
    tier_list = ('ndgf-t1',)
    split = None
    tier_based = False
    viewgroup = 'pub'
    sort = staticmethod(sorted)


    def retrieveWLCGData(self, start_date, end_date):

        end_date += " 23:59:59"

        columns=({'name': 'resource_type', 'code': "'compute'"}, wlcg.VO_NAME, wlcg.CORE_SECONDS_HS06, wlcg.CPU_SECONDS_HS06)
        group_by=(wlcg.VO_NAME, 'resource_type')
        self.wlcgdb.add_query(columns=columns, group_by=group_by, timerange=(start_date, end_date),
                              vo_list=self.vo_list, tier_list=self.tier_list)

        columns=({'name': 'resource_type', 'code': "'storage'"}, wlcg.VO_NAME, wlcg.DISK_USED, wlcg.TAPE_USED)
        group_by=(wlcg.VO_NAME, 'resource_type')
        self.wlcgdb.add_storage_query(columns=columns, group_by=group_by, end_time=(start_date, end_date),
                                      exclude_groups=('atlas-no','atlas-dk','UNKNOWN PROJECT','ops','behrmann','dteam'))

        columns=({'name': wlcg.VO_NAME, 'code': wlcg.VO_NAME},
                 {'name': CORE_DAYS_HS06, 'code': "sum(case when resource_type = 'compute' then %s / (24*3600) else 0 end)" % wlcg.CORE_SECONDS_HS06},
                 {'name': CPU_DAYS_HS06, 'code': "sum(case when resource_type = 'compute' then %s / (24*3600) else 0 end)" % wlcg.CPU_SECONDS_HS06},
                 {'name': DISK_TIB, 'code': "sum(case when resource_type = 'storage' then %s / 1099511627776 else 0 end)" %  wlcg.CORE_SECONDS_HS06},
                 {'name': TAPE_TIB, 'code': "sum(case when resource_type = 'storage' then %s / 1099511627776 else 0 end)" %  wlcg.CPU_SECONDS_HS06})
        self.wlcgdb.add_outer_query(columns=columns, group_by=(wlcg.VO_NAME,))

        d = self.wlcgdb.fetch()
        return d


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


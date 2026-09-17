#!/usr/bin/python

from copy import deepcopy
import psycopg2
from sgas.database.postgresql import database as pgdatabase


USER = 'global_user'
TIER = 'tier'
SITE = 'site'
COUNTRY = 'country'
MACHINE_NAME = 'machine_name'
YEAR = 'year'
MONTH = 'month'
TIME = 'end_time'
PROCESSORS = 'ncores'
NODE_COUNT = 'nodecount'
N_JOBS = 'n_jobs'
CORE_SECONDS = 'core_seconds'   # processors * wall_seconds
WALL_SECONDS = 'wall_seconds'
CPU_SECONDS = 'cpu_seconds'
CORE_SECONDS_HS06 = 'core_seconds_hs06'   # processors * wall_seconds_hs06
WALL_SECONDS_HS06 = 'wall_seconds_hs06'
CPU_SECONDS_HS06 = 'cpu_seconds_hs06'
HS06_CORE_EQUIVALENTS = 'hs06_core_equivalents'  # processors * wall_seconds_hs06 / (end_time - start_time)
HS06_CPU_EQUIVALENTS = 'hs06_cpu_equivalents'  # cpu_seconds_hs06 / (end_time - start_time)
CORE_EQUIVALENTS = 'core_equivalents'  # processors * wall_seconds / (end_time - start_time)
EFFICIENCY = 'efficiency'
VO_NAME = 'voname'
VO_GROUP = 'vo_group'
VO_ROLE = 'vo_role'

GROUP = 'group_identity'
RESOURCE_USED = 'resource_capacity_used'
DISK_USED = 'disk_used'
TAPE_USED = 'tape_used'


_groupable_columns = (USER, TIME, YEAR, MONTH, TIER, SITE, COUNTRY, VO_NAME, VO_GROUP, VO_ROLE, MACHINE_NAME, PROCESSORS, NODE_COUNT)
value_columns = (N_JOBS, CORE_SECONDS, WALL_SECONDS, CPU_SECONDS, CORE_SECONDS_HS06, WALL_SECONDS_HS06, CPU_SECONDS_HS06,
                  EFFICIENCY, HS06_CORE_EQUIVALENTS, HS06_CPU_EQUIVALENTS, CORE_EQUIVALENTS)

_st_groupable_col = (GROUP)
_st_value_col = (RESOURCE_USED, DISK_USED, TAPE_USED)


def _build_sql(column_list, from_table, group_by, timerange, end_time=None, joins=None, in_requirements=(), notin_requirements=()):

    sql = ""
    sql_args = []

    for c in column_list:
        if sql:
            sql += ',\n'
        else:
            sql = 'select\n'

        sql += '\t' + c['code'] + ' as ' + c['name']

    sql += '\nfrom %s' % from_table
    if joins:
        sql += joins.get()

    wheres = ""
    if timerange:
        wheres += "\tend_time between %s and %s"
        sql_args.append(timerange[0])
        sql_args.append(timerange[1])

    if end_time:
        if wheres: wheres += ' and\n'
        wheres += "\tend_time = (SELECT max(end_time) FROM storagerecords WHERE end_time >= %s AND end_time <= %s)"
        sql_args.append(end_time[0])
        sql_args.append(end_time[1])

    for req in in_requirements:
        name = req['name']
        in_list = req['list']
        if wheres: wheres += ' and\n'
        wheres += '\t' + name + ' in %s'
        sql_args.append(in_list)

    for req in notin_requirements:
        name = req['name']
        notin_list = req['list']
        if wheres: wheres += ' and\n'
        wheres += '\t' + name + ' not in %s'
        sql_args.append(notin_list)

    if wheres:
        sql += '\nwhere\n' + wheres

    if group_by:
        sql += '\ngroup by\n'
        first = True
        for g in group_by:
            if not first:
                sql += ',\n'
            first = False
            sql += '\t' + g

    return sql, sql_args


class _Joins():
    def __init__(self):
        self.joins = []

    def add(self, table, on):
        if table not in [j['table'] for j in self.joins]:
            self.joins.append({'table': table, 'on': on})

    def get(self):
        sql = ""
        for j in self.joins:
            sql += '\n\tleft join %s %s ' % (j['table'], j['on']) 
        return sql


class _DB():
    def __init__(self, db_host=None, db_name=None, db_username=None, db_password=None):
        self.con = psycopg2.connect(host=db_host, database=db_name, user=db_username, password=db_password)
        self.cur = self.con.cursor()

    def __del__(self):
        self.con.close()
        
    def query(self, sql, sql_args):
        msql = self.cur.mogrify(sql, sql_args)
        self.cur.execute(msql)
        return self.cur.fetchall()



class WLCG:
    
    def __init__(self, db=None, db_host=None, db_name=None, db_username=None, db_password=None):
        """
        The caller should provide either a pre-initialized 'db'-object, which has a 'query' method,
        or db_host, db_name, db_username and db_password for us to initialize a db session ourselves.
        """

        assert db or (db_host and db_name and db_username and db_password)

        if db:
            self.db = db
        else:
            self.db = _DB(db_host, db_name, db_username, db_password)

        self.query = ""
        self.query_args = []


    def fetch(self):

        if not self.query:
            return None

        #with open("/var/tmp/hepp", "w") as f:
        #    f.write(self.query + '\n')
        #    f.write(str(self.query_args))

        query = self.query + '\n;'
        query_args = deepcopy(self.query_args)
        self.query = ""
        self.query_args = []
        return self.db.query(query, query_args);


    def clear_query(self):
        self.query = ""
        self.query_args = []


    def add_outer_query(self, columns=(), group_by=()):

        assert self.query, "No inner query!"

        column_list = []

        for c in columns:
            assert type(c) == type({})
            assert 'name' in c
            assert 'code' in c
            column_list.append(c)

        sql = ""
        for c in columns:
            if sql:
                sql += ',\n'
            else:
                sql = 'select\n'

            sql += '\t' + c['code'] + ' as ' + c['name']

        sql += '\nfrom (\n%s\n) inner_query' % self.query

        if group_by:
            sql += '\ngroup by\n'
            first = True
            for g in group_by:
                if not first:
                    sql += ',\n'
                first = False
                sql += '\t' + g

        self.query = sql


    def add_storage_query(self, columns=(), group_by=(), end_time=None, exclude_groups=(), vo_list=()):
        column_list = []
        sql_args = []
        for c in columns:

            if type(c) == type({}):
                assert 'name' in c
                assert 'code' in c
                column_list.append(c)
            elif c == GROUP:
                column_list.append({'name': GROUP, 'code': 'group_identity'})
            elif c == VO_NAME:
                code = "case when group_identity like 'atlas-%%' then 'atlas' else group_identity end"
                column_list.append({'name': VO_NAME, 'code': code})
            elif c == RESOURCE_USED:
                code = 'resource_capacity_used'
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': RESOURCE_USED, 'code': code})
            elif c == DISK_USED:
                code = "case when storage_media = 'disk' then resource_capacity_used else 0 end"
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': DISK_USED, 'code': code})
            elif c == TAPE_USED:
                code = "case when storage_media = 'tape' then resource_capacity_used else 0 end"
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': TAPE_USED, 'code': code})
            else:
                assert False, 'Unknwon column %s' % c

        in_req = []
        if vo_list:
            in_req.append({'name': VO_NAME, 'list': vo_list})

        notin_req = []
        if exclude_groups:
            notin_req.append({'name': 'group_identity', 'list': exclude_groups})

        sql, args = _build_sql(column_list, 'storagerecords', group_by, timerange=None, end_time=end_time, in_requirements=in_req, notin_requirements=notin_req)
        sql_args += args

        if self.query:
            self.query += "\n\nunion all\n\n"
        self.query += sql
        self.query_args += sql_args


    def add_query(self, columns=(), group_by=(), timerange=None, vo_list=None, tier_list=None):
        joins = _Joins()
        column_list = []
        sql_args = []
        for c in columns:

            if type(c) == type({}):
                assert 'name' in c
                assert 'code' in c
                column_list.append(c)
            elif c == MACHINE_NAME:
                joins.add('machinename', 'on wlcg.usagedata.machine_name_id = machinename.id')
                column_list.append({'name': MACHINE_NAME, 'code': 'machinename.machine_name'})

            elif c == USER:
                joins.add('globalusername', 'on wlcg.usagedata.global_user_name_id = globalusername.id')
                column_list.append({'name': USER, 'code': 'globalusername.global_user_name'})

            elif c == SITE:
                joins.add('wlcg.machinename_site_junction', 'using(machine_name_id)')
                joins.add('wlcg.sites', 'using(site_id)')
                column_list.append({'name': SITE, 'code': 'wlcg.sites.site_name'})

            elif c == COUNTRY:
                joins.add('wlcg.machinename_site_junction', 'using(machine_name_id)')
                joins.add('wlcg.sites', 'using(site_id)')
                joins.add('wlcg.countries', 'using(country_id)')
                column_list.append({'name': COUNTRY, 'code': 'wlcg.countries.country_name'})

            elif c == TIER:
                joins.add('voinformation', 'on voinformation.id = wlcg.usagedata.vo_information_id')
                joins.add('wlcg.tiers', "on wlcg.tiers.machine_name_id = wlcg.usagedata.machine_name_id and (wlcg.tiers.vo_name = voinformation.vo_name or wlcg.tiers.vo_name = '*')")
                column_list.append({'name':TIER, 'code': 'wlcg.tiers.tier_name'})

            elif c == MONTH:
                column_list.append({'name':MONTH, 'code': 'extract(month from end_time)::integer'})

            elif c == YEAR:
                column_list.append({'name':YEAR, 'code': 'extract(year from end_time)::integer'})

            elif c == VO_NAME:
                joins.add('voinformation', 'on voinformation.id = wlcg.usagedata.vo_information_id')
                column_list.append({'name': VO_NAME, 'code': 'voinformation.vo_name'})

            elif c == VO_GROUP:
                joins.add('voinformation', 'on voinformation.id = wlcg.usagedata.vo_information_id')
                column_list.append({'name': VO_GROUP, 'code': 'voinformation.vo_attributes[1][1]'})

            elif c == VO_ROLE:
                joins.add('voinformation', 'on voinformation.id = wlcg.usagedata.vo_information_id')
                column_list.append({'name': VO_ROLE, 'code': 'voinformation.vo_attributes[1][2]'})

            elif c == PROCESSORS:
                column_list.append({'name':PROCESSORS, 'code': 'processors'})

            elif c == NODE_COUNT:
                column_list.append({'name':NODE_COUNT, 'code': 'node_count'})

            elif c == N_JOBS:
                code = 'count(*)' if group_by else '1'
                column_list.append({'name': N_JOBS, 'code': code})

            elif c == EFFICIENCY:
                if group_by:
                    code = 'case when sum(wall_duration) < 1 then null else sum(cpu_duration)*1.0/sum(wall_duration*processors) end'
                else:
                    code = 'case when wall_duration < 1 then 0 else cpu_duration*1.0/(wall_duration*processors) end'
                column_list.append({'name': EFFICIENCY, 'code': code})

            elif c == CPU_SECONDS:
                code = 'cpu_duration'
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': CPU_SECONDS, 'code': code})

            elif c == WALL_SECONDS:
                code = 'wall_duration'
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': WALL_SECONDS, 'code': code})

            elif c == CORE_SECONDS:
                code = 'wall_duration*processors'
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': CORE_SECONDS, 'code': code})

            elif c == WALL_SECONDS_HS06:
                code = 'wall_duration*hs06'
                if group_by: 
                    code = 'sum(' + code + ')'
                column_list.append({'name': WALL_SECONDS_HS06, 'code': code})

            elif c == CPU_SECONDS_HS06:
                code = 'cpu_duration*hs06'
                if group_by: 
                    code = 'sum(' + code + ')'
                column_list.append({'name': CPU_SECONDS_HS06, 'code': code})

            elif c == CORE_SECONDS_HS06:
                code = 'wall_duration*processors*hs06'
                if group_by: 
                    code = 'sum(' + code + ')'
                column_list.append({'name': CORE_SECONDS_HS06, 'code': code})

            elif c == HS06_CORE_EQUIVALENTS:
                if group_by:
                    code = 'sum(wall_duration*processors*hs06) / (extract(EPOCH from %s::timestamp) - extract(EPOCH from %s::timestamp))'
                else:
                    code = 'wall_duration*processors*hs06 / (extract(EPOCH from %s::timestamp) - extract(EPOCH from %s::timestamp))'
                column_list.append({'name': HS06_CORE_EQUIVALENTS, 'code': code})

                sql_args.append(timerange[1])
                sql_args.append(timerange[0])

            elif c == HS06_CPU_EQUIVALENTS:
                if group_by:
                    code = 'sum(cpu_duration*hs06) / (extract(EPOCH from %s::timestamp) - extract(EPOCH from %s::timestamp))'
                else:
                    code = 'cpu_duration*hs06 / (extract(EPOCH from %s::timestamp) - extract(EPOCH from %s::timestamp))'
                column_list.append({'name': HS06_CPU_EQUIVALENTS, 'code': code})

                sql_args.append(timerange[1])
                sql_args.append(timerange[0])

            elif c == CORE_EQUIVALENTS:
                if group_by:
                    code = 'sum(wall_duration*processors) / (extract(EPOCH from %s::timestamp) - extract(EPOCH from %s::timestamp))'
                else:
                    code = 'wall_duration*processors / (extract(EPOCH from %s::timestamp) - extract(EPOCH from %s::timestamp))'
                column_list.append({'name': CORE_EQUIVALENTS, 'code': code})

                sql_args.append(timerange[1])
                sql_args.append(timerange[0])

            else:
                assert False, 'Unknwon column %s' % c

        in_req = []
        if vo_list:
            joins.add('voinformation', 'on voinformation.id = wlcg.usagedata.vo_information_id')
            in_req.append({'name': 'voinformation.vo_name', 'list': vo_list})
        if tier_list:
            joins.add('voinformation', 'on voinformation.id = wlcg.usagedata.vo_information_id')
            joins.add('wlcg.tiers', "on wlcg.tiers.machine_name_id = wlcg.usagedata.machine_name_id and (wlcg.tiers.vo_name = voinformation.vo_name or wlcg.tiers.vo_name = '*')")
            in_req.append({'name': 'wlcg.tiers.tier_name', 'list': tier_list})

        sql, args = _build_sql(column_list, 'wlcg.usagedata', group_by, timerange, joins=joins,
                               in_requirements=in_req)
        sql_args += args

        if self.query:
            self.query += "\nunion\n"
        self.query += sql
        self.query_args += sql_args


def rowsToDicts(rows, columns):
    result = []
    for r in rows:
        d = {}
        for i in range(len(columns)):
            d[columns[i]] = r[i]
        result.append(d)
    return result

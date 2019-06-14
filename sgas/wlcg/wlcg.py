#!/usr/bin/python

import psycopg2
from sgas.database.postgresql import database as pgdatabase



USER = 'user'
TIER = 'tier'
SITE = 'site'
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
HS06_EQUIVALENTS = 'hs06_core_equivalents'  # processors * wall_seconds_hs06 / (end_time - start_time)
HS06_CPU_EQUIVALENTS = 'hs06_cpu_equivalents'  # cpu_seconds_hs06 / (end_time - start_time)
WALL_EQUIVALENTS = 'core_equivalents'  # processors * wall_seconds / (end_time - start_time)
EFFICIENCY = 'efficiency'
VO_NAME = 'voname'
VO_GROUP = 'vo_group'
VO_ROLE = 'vo_role'

_groupable_columns = (USER, TIME, YEAR, MONTH, TIER, SITE, VO_NAME, VO_GROUP, VO_ROLE, MACHINE_NAME, PROCESSORS, NODE_COUNT)
_value_columns = (N_JOBS, CORE_SECONDS, WALL_SECONDS, CPU_SECONDS, CORE_SECONDS_HS06, WALL_SECONDS_HS06, CPU_SECONDS_HS06,
                  EFFICIENCY, HS06_EQUIVALENTS, HS06_CPU_EQUIVALENTS, WALL_EQUIVALENTS)


_hostscalefactor_on = """on usagedata.machine_name_id = hostscalefactors_data.machine_name_id and
                            hostscalefactors_data.scalefactor_type_id = (
                                ( select
                                    hostscalefactor_types.id
                                  from hostscalefactor_types
                                  where hostscalefactor_types.factor_type = 'hepspec06' limit 1)
                            ) and usagedata.start_time <@ hostscalefactors_data.validity_period"""


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


class WLCG:
    
    def __init__(self, db=None, db_host=None, db_name=None, db_username=None, db_password=None):

        assert db or (db_host and db_name and db_username and db_password)

        if db:
            self.db = db
        else:
            db_url = ':'.join((db_host, '', db_name, db_username, db_password))
            self.db = pgdatabase.PostgreSQLDatabase(db_url)

    def fetch(self, columns=(), group_by=(), timerange=None, vo_list=None):
        joins = _Joins()
        column_list = []
        sql_args = []
        for c in columns:
            assert c in _groupable_columns or c in _value_columns

            if c == MACHINE_NAME:
                joins.add('machinename', 'on usagedata.machine_name_id = machinename.id')
                column_list.append({'name': MACHINE_NAME, 'code': 'machinename.machine_name'})

            elif c == USER:
                joins.add('globalusername', 'on usagedata.global_user_name_id = globalusername.id')
                column_list.append({'name': USER, 'code': 'globalusername.global_user_name'})

            elif c == SITE:
                joins.add('wlcg.machinename_site_junction', 'using(machine_name_id)')
                joins.add('wlcg.sites', 'using(site_id)')
                column_list.append({'name': SITE, 'code': 'wlcg.sites.site_name'})

            elif c == TIER:
                joins.add('voinformation', 'on voinformation.id = usagedata.vo_information_id')
                joins.add('wlcg.tiers', "on wlcg.tiers.machine_name_id = usagedata.machine_name_id and (wlcg.tiers.vo_name = voinformation.vo_name or wlcg.tiers.vo_name = '*')")
                column_list.append({'name':TIER, 'code': 'wlcg.tiers.tier_name'})

            elif c == MONTH:
                column_list.append({'name':MONTH, 'code': 'extract(month from end_time)::integer'})

            elif c == YEAR:
                column_list.append({'name':YEAR, 'code': 'extract(year from end_time)::integer'})

            elif c == VO_NAME:
                joins.add('voinformation', 'on voinformation.id = usagedata.vo_information_id')
                column_list.append({'name': VO_NAME, 'code': 'voinformation.vo_name'})

            elif c == VO_GROUP:
                joins.add('voinformation', 'on voinformation.id = usagedata.vo_information_id')
                column_list.append({'name': VO_GROUP, 'code': 'voinformation.vo_attributes[1][1]'})

            elif c == VO_ROLE:
                joins.add('voinformation', 'on voinformation.id = usagedata.vo_information_id')
                column_list.append({'name': VO_ROLE, 'code': 'voinformation.vo_attributes[1][2]'})

            elif c == PROCESSORS:
                column_list.append({'name':PROCESSORS, 'code': 'coalesce(usagedata.processors,1)'})

            elif c == NODE_COUNT:
                column_list.append({'name':NODE_COUNT, 'code': 'coalesce(usagedata.node_count,1)'})

            elif c == N_JOBS:
                code = 'count(*)' if group_by else '1'
                column_list.append({'name': N_JOBS, 'code': code})

            elif c == EFFICIENCY:
                code = 'case when wall_seconds < 1 then null else cpu_seconds*1.0/wall_seconds end'
                column_list.append({'name': EFFICIENCY, 'code': code})

            elif c == CPU_SECONDS:
                code = 'coalesce(usagedata.cpu_duration,0)'
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': CPU_SECONDS, 'code': code})

            elif c == WALL_SECONDS:
                code = 'coalesce(usagedata.wall_duration,0)'
                if group_by:
                    code = 'sum(' + code + ')'
                column_list.append({'name': WALL_SECONDS, 'code': code})

            elif c == WALL_SECONDS_HS06:
                code = 'coalesce(usagedata.wall_duration,0)*coalesce(hostscalefactors_data.scale_factor,1.0)'
                if group_by: 
                    code = 'sum(' + code + ')'
                column_list.append({'name': WALL_SECONDS_HS06, 'code': code})

                joins.add('hostscalefactors_data', _hostscalefactor_on)

            elif c == CPU_SECONDS_HS06:
                code = 'coalesce(usagedata.cpu_duration,0)*coalesce(hostscalefactors_data.scale_factor,1.0)'
                if group_by: 
                    code = 'sum(' + code + ')'
                column_list.append({'name': CPU_SECONDS_HS06, 'code': code})

                joins.add('hostscalefactors_data', _hostscalefactor_on)

            elif c == CORE_SECONDS_HS06:
                code = 'coalesce(usagedata.wall_duration,0)*coalesce(usagedata.processors,1)*coalesce(hostscalefactors_data.scale_factor,1.0)'
                if group_by: 
                    code = 'sum(' + code + ')'
                column_list.append({'name': CORE_SECONDS_HS06, 'code': code})

                joins.add('hostscalefactors_data', _hostscalefactor_on)

            elif c == HS06_EQUIVALENTS:
                if group_by:
                    code = 'sum(coalesce(usagedata.wall_duration,0)*coalesce(usagedata.processors,1))*coalesce(hostscalefactors_data.scale_factor,1.0) / (extract(epoch from %s) - extract(epoch from %s))'
                else:
                    code = 'coalesce(usagedata.wall_duration,0)*coalesce(usagedata.processors,1)*coalesce(hostscalefactors_data.scale_factor,1.0) / (extract(epoch from %s) - extract(epoch from %s))'
                column_list.append({'name': HS06_EQUIVALENTS, 'code': code})

                joins.add('hostscalefactors_data', _hostscalefactor_on)
                sql_args.append(timerange[1])
                sql_args.append(timerange[0])

            elif c == HS06_CPU_EQUIVALENTS:
                if group_by:
                    code = 'sum(coalesce(usagedata.cpu_duration,0)*coalesce(hostscalefactors_data.scale_factor,1.0) / (extract(epoch from %s) - extract(epoch from %s))'
                else:
                    code = 'coalesce(usagedata.cpu_duration,0)*coalesce(hostscalefactors_data.scale_factor,1.0) / (extract(epoch from %s) - extract(epoch from %s))'
                column_list.append({'name': HS06_CPU_EQUIVALENTS, 'code': code})

                joins.add('hostscalefactors_data', _hostscalefactor_on)
                sql_args.append(timerange[1])
                sql_args.append(timerange[0])

            elif c == WALL_EQUIVALENTS:
                if group_by:
                    code = 'sum(coalesce(usagedata.wall_duration,0)*coalesce(usagedata.processors,1)) / (extract(epoch from %s) - extract(epoch from %s))'
                else:
                    code = 'coalesce(usagedata.wall_duration,0)*coalesce(usagedata.processors,1) / (extract(epoch from %s) - extract(epoch from %s))'
                column_list.append({'name': WALL_EQUIVALENTS, 'code': code})

                sql_args.append(timerange[1])
                sql_args.append(timerange[0])

            else:
                assert False, 'How did we end up here?'

        sql = ""

        for c in column_list:
            if sql:
                sql += ',\n'
            else:
                sql = 'select\n'

            sql += '\t' + c['code'] + ' as ' + c['name']
        
        sql += '\nfrom usagedata' + joins.get()

        wheres = ""
        if timerange:
            wheres += "\tend_time between %s and %s"
            sql_args.append(timerange[0])
            sql_args.append(timerange[1])

        if vo_list:
            assert VO_NAME in columns
            if wheres: wheres += ' and\n'
            wheres += '\tvoinformation.vo_name in %s'
            sql_args.append(vo_list)

        if wheres:
            sql += '\nwhere\n' + wheres

        if group_by:
            sql += '\ngroup by\n'
            first = True
            for g in group_by:
                assert g in columns, '%s in group_by not in columns' % g
                assert g in _groupable_columns

                if not first:
                    sql += ',\n'
                first = False
                sql += '\t' + g
        sql += '\n;'

        result = []
        for row in self.db.query(sql, sql_args):
            d = {}
            for i in range(len(columns)):
                d[columns[i]] = row[i]
            result.append(d)
        return result

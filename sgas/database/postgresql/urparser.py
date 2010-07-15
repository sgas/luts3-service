"""
Parser for converting usage records into statements for inserting data into
PostgreSQL.

Author: Henrik Thostrup Jensen
Copyright: Nordic Data Grid Facility (2010)
"""

import time

from sgas.usagerecord import ursplitter, urparser


ARG_LIST = [
    'record_id',
    'create_time',
    'global_job_id',
    'local_job_id',
    'local_user_id',
    'global_user_name',
    'vo_type',
    'vo_issuer',
    'vo_name',
    'vo_attributes',
    'machine_name',
    'job_name',
    'charge',
    'status',
    'queue',
    'host',
    'node_count',
    'processors',
    'project_name',
    'submit_host',
    'start_time',
    'end_time',
    'submit_time',
    'cpu_duration',
    'wall_duration',
    'ksi2k_cpu_duration',
    'ksi2k_wall_duration',
    'user_time',
    'kernel_time',
    'major_page_faults',
    'runtime_environments',
    'exit_code',
    'insert_hostname',
    'insert_identity',
    'insert_time'
]



def buildArgList(usagerecord_data, insert_identity=None, insert_hostname=None):

    args = []

    insert_time = time.gmtime()

    for ur_element in ursplitter.splitURDocument(usagerecord_data):
        ur_doc = urparser.xmlToDict(ur_element,
                                    insert_identity=insert_identity,
                                    insert_hostname=insert_hostname,
                                    insert_time=insert_time)

        # convert vo attributes into postgresql arrays
        if 'vo_attrs' in ur_doc:
            vo_attrs = [ [ e.get('group'), e.get('role') ] for e in ur_doc['vo_attrs'] ]
            #print vo_attrs
            ur_doc['vo_attributes'] ='{' + ','.join([ '{' + ','.join( [ '"' + f + '"' if f else 'null' for f in e  ] ) + '}' for e in vo_attrs ]) + '}'

        arg = [ ur_doc.get(a, None) for a in ARG_LIST ]
        args.append(arg)

    return args


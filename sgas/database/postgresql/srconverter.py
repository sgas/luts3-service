"""
Parser for converting storage records into statements for inserting data into
PostgreSQL.

Author: Henrik Thostrup Jensen
Copyright: NorduNET / Nordic Data Grid Facility (2010, 2011)
"""

from twisted.python import log



ARG_LIST = [
    'record_id',
    'create_time',
    'storage_system',
    'storage_share',
    'storage_media',
    'storage_class',
    'file_count',
    'directory_path',
    'local_user',
    'local_group',
    'user_identity',
    'group',
    'group_attribute',
    'measure_time',
    'valid_duration',
    'resource_capacity_used',
    'logical_capacity_used',
    'insert_hostname',
    'insert_identity',
    'insert_time'
]



def createInsertArguments(storagerecord_docs, insert_identity=None, insert_hostname=None):

    stringify = lambda f : str(f) if f is not None else None

    args = []

    for sr_doc in storagerecord_docs:

        # convert group attributes into arrays (adaption is done by db layer)
        if 'group_attribute' in sr_doc:
            sr_doc['group_attribute'] = [ list(attrs) for attrs in sr_doc['group_attribute'] ]

        arg = [ sr_doc.get(a, None) for a in ARG_LIST ]
        args.append(arg)

    return args


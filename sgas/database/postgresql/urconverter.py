"""
Parser for converting usage records into statements for inserting data into
PostgreSQL.

Author: Henrik Thostrup Jensen
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log



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
    'user_time',
    'kernel_time',
    'major_page_faults',
    'runtime_environments',
    'exit_code',
    'downloads',
    'uploads',
    'insert_hostname',
    'insert_identity',
    'insert_time'
]



def createInsertArguments(usagerecord_docs, insert_identity=None, insert_hostname=None):

    stringify = lambda f : str(f) if f is not None else None

    args = []

    for ur_doc in usagerecord_docs:

        # hack for dealing with bad local job ids (occurs from time to time in ARC)
        if 'local_job_id' in ur_doc:
            lji = ur_doc['local_job_id']
            # heuristic for checking for bad local job id
            # AFAIK this catches all the bad local job ids i've seen so far, with no false positives
            if len(lji) > 40 or lji.startswith('/'):
                ur_doc['local_job_id'] = None
                # the record id is typically machine_name:local_job_id, so we need to change that as well
                old_record_id = ur_doc['record_id']
                if 'global_job_id' in ur_doc:
                    # if available, just use the global job, this is a good value to use
                    ur_doc['record_id'] = ur_doc['global_job_id']
                else:
                    # if no global job id is avalable, use the createtime as it is fairly unique and required to exist
                    ur_doc['record_id'] = ur_doc.get('machine_name', '') + ':' + ur_doc.get('create_time', '')
                # finally, log that the heuristic was used
                log.msg('HEURISTIC IN USE. Removed LocalJobId and rewrote recordId from %s to %s' % (old_record_id, ur_doc['record_id']))
        # end hack :-)

        # from version 3.5 the db schema only does integers
        if 'charge' in ur_doc:
            ur_doc['charge'] = int(ur_doc['charge'])

        if 'exit_code' in ur_doc:
            ur_doc['exit_code'] = ur_doc['exit_code'] & 0377 # equivalent to modulus 256

        if 'host' in ur_doc and len(ur_doc['host']) > 2700:
            ur_doc['host'] = ur_doc['host'][:2699] + '$' # dollar marks that the string has been chopped

        # convert vo attributes into arrays (adaption is done by psycopg2)
        if 'vo_attrs' in ur_doc:
            vo_attrs = [ [ e.get('group'), e.get('role') ] for e in ur_doc['vo_attrs'] ]
            #ur_doc['vo_attributes'] = [ [ str(f) if f else None for f in e ] for e in vo_attrs ]
            ur_doc['vo_attributes'] = [ [ stringify(f) for f in e ] for e in vo_attrs ]

        if 'downloads' in ur_doc:
            dls = []
            for dl in ur_doc['downloads']:
                dla = dl.get('url'), dl.get('size'), dl.get('start_time'), dl.get('end_time'), dl.get('bypass_cache'), dl.get('from_cache')
                dls.append(dla)
            ur_doc['downloads'] = [ [ stringify(f) for f in e  ] for e in dls ]

        if 'uploads' in ur_doc:
            uls = []
            for ul in ur_doc['uploads']:
                ula = ul.get('url'), ul.get('size'), ul.get('start_time'), ul.get('end_time')
                uls.append(ula)
            ur_doc['uploads'] = [ [ stringify(f) for f in e  ] for e in uls ]

        arg = [ ur_doc.get(a, None) for a in ARG_LIST ]
        args.append(arg)

    return args


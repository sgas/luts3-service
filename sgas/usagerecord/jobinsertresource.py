"""
Insertion resources for SGAS.

Used for inserting usage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
        Magnus Jonsson <magnus@hpc2n.umu.se>
Copyright: NorduNET / Nordic Data Grid Facility (2009, 2010, 2011)
"""

from twisted.internet import defer
from twisted.python import log
from twisted.enterprise import adbapi

import time
import re

import psycopg2
import psycopg2.extensions # not used, but enables tuple adaption

from sgas.authz import rights, ctxinsertchecker
from sgas.generic.insertresource import GenericInsertResource
from sgas.database import error as dberror
from sgas.usagerecord import ursplitter, urparser, urconverter

from sgas.usagerecord import updater


ACTION_INSERT           = 'insert' # backwards compat
ACTION_JOB_INSERT       = 'jobinsert'
CTX_MACHINE_NAME        = 'machine_name'

ARCID_RE = re.compile("[a-zA-Z]+://([-a-zA-Z0-9._]*)(:[0-9]+)?/[a-zA-Z0-9]+/([a-zA-Z0-9_-]+)")

class JobInsertChecker(ctxinsertchecker.InsertChecker):

    CONTEXT_KEY = CTX_MACHINE_NAME

class JobUsageRecordInsertResource(GenericInsertResource):
    
    PLUGIN_ID   = 'ur'
    PLUGIN_NAME = 'Registration' 

    authz_right = ACTION_JOB_INSERT
    insert_error_msg = 'Error during job usage insert: %s'
    insert_authz_reject_msg = 'Rejecting job usage insert for %s. No insert rights.'

    def __init__(self, cfg, db, authorizer):
        GenericInsertResource.__init__(self,db,authorizer)
        authorizer.addChecker(self.authz_right, JobInsertChecker(authorizer.insert_check_depth))
        authorizer.rights.addActions(ACTION_INSERT)
        authorizer.rights.addOptions(ACTION_INSERT,rights.OPTION_ALL)
        
        authorizer.rights.addActions(ACTION_JOB_INSERT)
        authorizer.rights.addOptions(ACTION_JOB_INSERT,[ rights.OPTION_ALL ])
        authorizer.rights.addContexts(ACTION_JOB_INSERT,[ CTX_MACHINE_NAME ])
        
        self.updater = updater.AggregationUpdater(db)
        db.attachService(self.updater)

    def insertRecords(self, data, subject, hostname):
        return self._insertJobUsageRecords(data, self.db, self.authorizer, subject, hostname)

    def _insertJobUsageRecords(self, usagerecord_data, db, authorizer, insert_identity=None, insert_hostname=None):

        # parse ur data
        insert_time = time.gmtime()

        ur_docs = []

        for ur_element in ursplitter.splitURDocument(usagerecord_data):
            ur_doc = urparser.xmlToDict(ur_element,
                                    insert_identity=insert_identity,
                                    insert_hostname=insert_hostname,
                                    insert_time=insert_time)

            if 'machine_name' not in ur_doc:

                # Try to figure out the machine_name from the global_job_id
                m = ARCID_RE.match(ur_doc.get('global_job_id') or '')
                if m:
                    machine_name = m.group(1)
                    s = m.group(3)
                    record_id = 'ur-' + machine_name + '-' + s
                    log.msg("Job %s / %s had no machine_name; it will be assumed to be %s. Also, setting record_id to %s" % (ur_doc['record_id'], ur_doc['global_job_id'], machine_name, record_id))
                    ur_doc['machine_name'] = machine_name
                    ur_doc['record_id'] = record_id
                else:
                    raise Exception("UR %s / %s from %s at %s has no machine_name" % (ur_doc['record_id'], ur_doc.get('global_job_id'), insert_identity, ur_doc['end_time']))

            ur_docs.append(ur_doc)

        # check authz
        machine_names = set( [ doc.get(CTX_MACHINE_NAME) for doc in ur_docs ] )
        ctx = [ (CTX_MACHINE_NAME, mn) for mn in machine_names ]

        if authorizer.isAllowed(insert_identity, ACTION_JOB_INSERT, ctx):
            return self.insertJobUsageRecords(db,ur_docs)
        else:
            MSG = 'Subject %s is not allowed to perform insertion for machines: %s' % (insert_identity, ','.join(machine_names))
            return defer.fail(dberror.SecurityError(MSG))

    def insertJobUsageRecords(self, db, usagerecord_docs, retry=False):

        arg_list = urconverter.createInsertArguments(usagerecord_docs)
        
        r = db.recordInserter('usage', 'urcreate', arg_list)
        self.updater.updateNotification()
        return r        

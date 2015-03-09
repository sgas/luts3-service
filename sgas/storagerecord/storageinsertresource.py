"""
Insertion resources for SGAS.

Used for inserting storage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: NorduNET / Nordic Data Grid Facility (2009, 2010, 2011)
"""
from twisted.internet import defer
from twisted.python import log
from twisted.enterprise import adbapi

import time

import psycopg2
import psycopg2.extensions # not used, but enables tuple adaption

from sgas.authz import rights, ctxinsertchecker
from sgas.generic.insertresource import GenericInsertResource
from sgas.database import inserter, error as dberror
from sgas.storagerecord import srsplitter, srparser, srconverter

ACTION_STORAGE_INSERT   = 'storageinsert'
CTX_STORAGE_SYSTEM  = 'storage_system'

class StorageInsertChecker(ctxinsertchecker.InsertChecker):

    CONTEXT_KEY = CTX_STORAGE_SYSTEM

class StorageUsageRecordInsertResource(GenericInsertResource):
    
    PLUGIN_ID   = 'sr'
    PLUGIN_NAME = 'StorageRegistration'     

    authz_right = ACTION_STORAGE_INSERT
    insert_error_msg = 'Error during storage usage insert: %s'
    insert_authz_reject_msg = 'Rejecting storage usage insert for %s. No insert rights.'
    
    def __init__(self, cfg, db, authorizer):
        GenericInsertResource.__init__(self,db,authorizer)
        authorizer.addChecker(self.authz_right, StorageInsertChecker(authorizer.insert_check_depth))
        authorizer.rights.addActions(ACTION_STORAGE_INSERT)
        authorizer.rights.addOptions(ACTION_STORAGE_INSERT,[ rights.OPTION_ALL ])
        authorizer.rights.addContexts(ACTION_STORAGE_INSERT,[ CTX_STORAGE_SYSTEM ])

    def insertRecords(self, data, subject, hostname):
        return self._insertStorageUsageRecords(data, self.db, self.authorizer, subject, hostname)

    def _insertStorageUsageRecords(self, storagerecord_data, db, authorizer, insert_identity=None, insert_hostname=None):
        
        insert_time = time.gmtime()

        sr_docs = []

        for sr_element in srsplitter.splitSRDocument(storagerecord_data):
            sr_doc = srparser.xmlToDict(sr_element,
                                    insert_identity=insert_identity,
                                    insert_hostname=insert_hostname,
                                    insert_time=insert_time)
            sr_docs.append(sr_doc)

        storage_systems = set( [ doc.get('storage_system') for doc in sr_docs ] )
        ctx = [ ('storage_system', ss) for ss in storage_systems ]

        if authorizer.isAllowed(insert_identity, ACTION_STORAGE_INSERT, ctx):
            return self.insertStorageUsageRecords(db, sr_docs)
        else:
            MSG = 'Subject %s is not allowed to perform insertion for storage systems: %s' % (insert_identity, ','.join(storage_systems))
            return defer.fail(dberror.SecurityError(MSG))
        
        
    def insertStorageUsageRecords(self, db, storagerecord_docs, retry=False):
        
        arg_list = srconverter.createInsertArguments(storagerecord_docs)
               
        return db.recordInserter('storage usage', 'srcreate', arg_list)

"""
UR insert logic.

Should probably have its own top level module.

Author: Henrik Thostrup Jensen <htj@ndgf.org>

Copyright: Nordic Data Grid Facility (2010)
"""

import time

from twisted.internet import defer

from sgas.usagerecord import ursplitter, urparser
from sgas.storagerecord import srsplitter, srparser
from sgas.authz import rights
from sgas.database import error



def insertJobUsageRecords(usagerecord_data, db, authorizer, insert_identity=None, insert_hostname=None):

    # parse ur data
    insert_time = time.gmtime()

    ur_docs = []

    for ur_element in ursplitter.splitURDocument(usagerecord_data):
        ur_doc = urparser.xmlToDict(ur_element,
                                    insert_identity=insert_identity,
                                    insert_hostname=insert_hostname,
                                    insert_time=insert_time)
        ur_docs.append(ur_doc)

    # check authz
    machine_names = set( [ doc.get('machine_name') for doc in ur_docs ] )
    ctx = [ ('machine_name', mn) for mn in machine_names ]

    if authorizer.isAllowed(insert_identity, rights.ACTION_JOB_INSERT, ctx):
        return db.insertJobUsageRecords(ur_docs)
    else:
        MSG = 'Subject %s is not allowed to perform insertion for machines: %s' % (insert_identity, ','.join(machine_names))
        return defer.fail(error.SecurityError(MSG))



def insertStorageUsageRecords(storagerecord_data, db, authorizer, insert_identity=None, insert_hostname=None):

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

    if authorizer.isAllowed(insert_identity, rights.ACTION_STORAGE_INSERT, ctx):
        return db.insertStorageUsageRecords(sr_docs)
    else:
        MSG = 'Subject %s is not allowed to perform insertion for storage systems: %s' % (insert_identity, ','.join(storage_systems))
        return defer.fail(error.SecurityError(MSG))


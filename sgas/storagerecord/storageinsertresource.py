"""
Insertion resources for SGAS.

Used for inserting storage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: NorduNET / Nordic Data Grid Facility (2009, 2010, 2011)
"""

from sgas.authz import rights
from sgas.generic.insertresource import GenericInsertResource
from sgas.database import inserter

class StorageUsageRecordInsertResource(GenericInsertResource):

    authz_right = rights.ACTION_STORAGE_INSERT
    insert_error_msg = 'Error during storage usage insert: %s'
    insert_authz_reject_msg = 'Rejecting storage usage insert for %s. No insert rights.'

    def insertRecords(self, data, subject, hostname):
        d = inserter.insertStorageUsageRecords(data, self.db, self.authorizer, subject, hostname)
        return d


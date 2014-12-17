"""
Insertion resources for SGAS.

Used for inserting usage records into database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
        Magnus Jonsson <magnus@hpc2n.umu.se>
Copyright: NorduNET / Nordic Data Grid Facility (2009, 2010, 2011)
"""

from sgas.authz import rights
from sgas.generic.insertresource import GenericInsertResource
from sgas.database import inserter, error as dberror

class JobUsageRecordInsertResource(GenericInsertResource):

    authz_right = rights.ACTION_JOB_INSERT
    insert_error_msg = 'Error during job usage insert: %s'
    insert_authz_reject_msg = 'Rejecting job usage insert for %s. No insert rights.'


    def insertRecords(self, data, subject, hostname):
        d = inserter.insertJobUsageRecords(data, self.db, self.authorizer, subject, hostname)
        return d

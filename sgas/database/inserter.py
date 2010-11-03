"""
UR insert logic.

Should probably have its own top level module.

Author: Henrik Thostrup Jensen <htj@ndgf.org>

Copyright: Nordic Data Grid Facility (2010)
"""


from twisted.internet import defer

from sgas.authz import rights
from sgas.database import error
from sgas.database.postgresql import urparser



def insertRecords(ur_data, db, authorizer, insert_identity=None, insert_hostname=None):

    # 1. parse ur data
    # 2. check authz
    # 3. insert records

    # check the consistency between machine_name in records and the identity of the inserter

    arg_list = urparser.buildArgList(ur_data, insert_identity=insert_identity, insert_hostname=insert_hostname)

    docs = [ dict(zip(urparser.ARG_LIST, args)) for args in arg_list ]

    machine_names = set( [ doc.get('machine_name') for doc in docs ] )

    ctx = [ ('machine_name', mn) for mn in machine_names ]

    if authorizer.isAllowed(insert_identity, rights.ACTION_INSERT, ctx):
        d = db.insert(ur_data, insert_identity=insert_identity, insert_hostname=insert_hostname)
        return d
    else:
        MSG = 'Subject %s is not allowed to perform insertion for machines: %s' % (insert_identity, ','.join(machine_names))
        return defer.failure(error.SecurityError(MSG))



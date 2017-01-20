"""
Host scale factor updater.

This module will connect to the database (once available), and update the
scaling factors, in the database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.python import log
from twisted.internet import defer
from twisted.application import service



TRUNCATE_HOST_SCALE_FACTOR = '''TRUNCATE TABLE hostscalefactors'''
INSERT_HOST_SCALE_FACTOR   = '''INSERT INTO hostscalefactors (machine_name, scale_factor) VALUES (%s, %s)'''

# scale options
SCALE_BLOCK      = 'hostscaling'

class HostScaleFactorUpdater(service.Service):

    def __init__(self, cfg, db):
        pass


    def startService(self):
        raise Exception("The hostscalefactors plugin is obsolete. Use sgas-hs-tool instead!")


    def stopService(self):
        pass

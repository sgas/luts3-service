"""
Aggregation table updater.

This module will listen for update notifications and will thereafter update
the aggregation table in PostgreSQL. As this operation can take some time,
the operation cannot be done in insertion, and must hence be done
asyncrhronously and a bit clever.

Originally it was planned to use the NOTIFY/LISTEN functionality in PostgreSQL,
but I could not make it work using adbapi (and it appears other have this
problem as well).

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import psycopg2

from twisted.python import log
from twisted.internet import defer, reactor
from twisted.application import service
from twisted.enterprise import adbapi



SQL_SERIALIZABLE_TRANSACTION = '''SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'''



class AggregationUpdater(service.Service):

    def __init__(self, pool_proxy):

        self.pool_proxy = pool_proxy

        self.need_update = False
        self.updating    = False
        self.stopping    = False
        self.update_call = None
        self.update_def  = None


    def startService(self):
        service.Service.startService(self)
        # we might have been shutdown while some updates where pending,
        # or some records could have been inserted outside SGAS, so we
        # always schedule an update when starting
        self.scheduleUpdate()
        return defer.succeed(None)


    def stopService(self):
        self.stopping = True
        service.Service.stopService(self)
        if self.update_call is not None:
            self.update_call.cancel()
        if self.update_def is not None:
            return self.update_def
        else:
            return defer.succeed(None)


    def updateNotification(self):
        # the database need to be updated, schedule call
        self.need_update = True
        self.scheduleUpdate()


    def scheduleUpdate(self, delay=20):
        # only schedule call if no other call is planned
        if self.update_call is None:
            log.msg('Scheduling update for aggregated table in %i seconds.' % delay, system='sgas.AggregationUpdater')
            self.update_call = reactor.callLater(delay, self.performUpdate, True)


    def performUpdate(self, remove_call=False):
        if remove_call:
            self.update_call = None
        if self.updating:
            # if an update is running, defer this update to (much) later
            self.scheduleUpdate(delay=120)
            return defer.succeed(None)
        else:
            d = self.update()
            self.update_def = d
            return d

    # -- end scheduling logic

    @defer.inlineCallbacks
    def update(self):
        # will update the parts of the aggregated data table which has been
        # specified to need an update in the update table

        self.updating = True
        self.has_reconnected = False

        try:
            conn = adbapi.Connection(self.pool_proxy.dbpool)
            while True:
                try:
                    txn = adbapi.Transaction(self, conn)

                    # the update_uraggregate function requires serializable isolation level
                    # in order to execute correctly
                    txn.execute(SQL_SERIALIZABLE_TRANSACTION)
                    yield txn.callproc('update_uraggregate')
                    idmn = txn.fetchall()
                    txn.close()
                    conn.commit()

                    if idmn in (None, [(None,)]): # empty array -> response when all done
                        break
                    else:
                        insert_date, machine_name = idmn[0][0]
                        log.msg('Aggregation updated: %s / %s' % (insert_date, machine_name), system='sgas.AggregationUpdater')
                    if self.stopping:
                        break
                except psycopg2.InterfaceError, e:
                    # typically means we lost the connection due to a db restart
                    if self.has_reconnected:
                        log.msg('Reconnect in update failed, bailing out.', system='sgas.AggregationUpdater')
                        raise
                    else:
                        log.msg('Got InterfaceError while attempting update: %s.' % str(e), system='sgas.AggregationUpdater')
                        log.msg('Attempting reconnect.', system='sgas.AggregationUpdater')
                        self.has_reconnected = True
                        self.pool_proxy.reconnect()
                except:
                    conn.rollback()
                    raise

        except Exception, e:
            log.err(e, system='sgas.AggregationUpdater')
            raise

        finally:
            self.updating = False
            conn.close()


    def rebuild(self):
        # will perform a full rebuild of the data in the aggegration table
        # note that this is can be a fairly heavy operation often taking
        # several minutes (increasing the work_mem parameter can often lower
        # the time required)

        # not quite there yet
        pass


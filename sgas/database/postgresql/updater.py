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

from twisted.python import log
from twisted.internet import defer, reactor
from twisted.application import service



AGGREGATE_UPDATE_QUERY = '''SELECT * FROM uraggregated_update'''

DELETE_AGGREGATED_UPDATE = '''DELETE FROM uraggregated_update WHERE insert_time = %s AND machine_name = %s'''
DELETE_AGGREGATED_INFO = '''DELETE FROM uraggregated WHERE insert_time = %s AND machine_name = %s'''

# a nice small insert statement
UPDATE_AGGREGATED_INFO = '''
INSERT INTO uraggregated SELECT
    COALESCE(end_time::DATE, create_time::DATE),
    insert_time::DATE,
    machine_name,
    COALESCE(global_user_name, machine_name || ':' || local_user_id),
    CASE WHEN vo_issuer LIKE 'file:///%%'
        THEN NULL
        ELSE vo_issuer
    END,
    CASE WHEN vo_name is NULL
        THEN COALESCE(machine_name || ':' || project_name)
        ELSE
            CASE WHEN vo_name LIKE '/%%'
                THEN NULL
                ELSE vo_name
            END
    END,
    vo_attributes[1][1],
    vo_attributes[1][2],
    count(*),
    SUM(COALESCE(cpu_duration,0))  / 3600.0,
    SUM(COALESCE(wall_duration * COALESCE(processors,0))) / 3600.0,
    now()
FROM
    usagerecords
WHERE
    insert_time::date = %s AND machine_name = %s
GROUP BY
    COALESCE(end_time::DATE, create_time::DATE),
    insert_time::DATE,
    machine_name,
    COALESCE(global_user_name, machine_name || ':' || local_user_id),
    vo_issuer,
    CASE WHEN vo_name is NULL
        THEN COALESCE(machine_name || ':' || project_name)
        ELSE
            CASE WHEN vo_name LIKE '/%%'
                THEN NULL
                ELSE vo_name
            END
    END,
    vo_attributes
;'''



class AggregationUpdater(service.Service):

    def __init__(self, dbpool):

        self.dbpool = dbpool

        self.need_update = False
        self.updating    = False
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
            log.msg('Scheduling update for aggregated table in 20 seconds.')
            self.update_call = reactor.callLater(delay, self.performUpdate, True)


    def performUpdate(self, remove_call=False):
        if remove_call:
            self.update_call = None
        if self.updating:
            # if an update is running, defer this update to later
            self.scheduleUpdate(delay=30)
            return defer.succeed(None)
        else:
            d = self.update()
            self.update_def = d
            return d

    # -- end scheduling logic

    def _performUpdate(self, txn, insert_date, machine_name):
        # this is performed outside the main twisted thread and runs in a blocking manner
        # i.e., no deferreds should be used
        # furthermore the function is run within a transaction, so if any errors occurs
        # everything is rolled back
        txn.execute(DELETE_AGGREGATED_INFO, (insert_date, machine_name))
        txn.execute(UPDATE_AGGREGATED_INFO, (insert_date, machine_name))
        txn.execute(DELETE_AGGREGATED_UPDATE, (insert_date, machine_name))


    @defer.inlineCallbacks
    def update(self):
        # will update the parts of the aggregated data table which has been
        # specified to need an update in the update table

        self.updating = True

        try:
            # first we get the info for what should be updated
            rows = yield self.dbpool.runQuery(AGGREGATE_UPDATE_QUERY)
            n_rows = len(rows)
            #print 'Updates: %i to be performed' % n_rows
            # this strategy may need to be updated at some time...
            for row in rows:
                insert_date, machine_name = row
                insert_date = str(insert_date)

                yield self.dbpool.runInteraction(self._performUpdate, insert_date, machine_name)

        except Exception, e:
            print e
            print dir(e)
            raise

        finally:
            self.updating = False


    def rebuild(self):
        # will perform a full rebuild of the data in the aggegration table
        # note that this is can be a fairly heavy operation often taking
        # several minutes (increasing the work_mem parameter can often lower
        # the time required)

        # not quite there yet
        pass


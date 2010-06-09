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
from twisted.internet import defer, reactor, task
from twisted.application import service

from sgas.database import error

UPDATE_INFO_QUERY = '''SELECT * FROM uraggregated_update'''
TRUNCATE_UPDATE_TABLE = '''TRUNCATE uraggregated_update'''

DELETE_AGGREGATED_INFO = """DELETE FROM uraggregated_update WHERE insert_time = %s AND host = %s"""

# a nice small insert statement
UPDATE_AGGREGATED_INFO = '''
INSERT INTO uraggregated SELECT
    CASE WHEN end_time IS NULL
        THEN create_time::DATE
        ELSE end_time::DATE
    END                                 AS execution_date,
    insert_time::DATE                   AS insert_date,
    machine_name,
    CASE WHEN global_user_name ISNULL
        THEN (machine_name || ':' || local_user_id)
        ELSE global_user_name
    END                                 AS user_identity,
        vo_issuer AS vo_issuer,
        vo_name AS vo_name,
    vo_attributes[1][1]                 AS vo_group,
    vo_attributes[1][2]                 AS vo_role,
    count(*)                            AS n_jobs,
    SUM(COALESCE(cpu_duration,0))       AS sum_cputime,
    SUM(COALESCE(wall_duration,0))      AS sum_walltime,
    now()                               AS generate_time
FROM
    usagerecords
WHERE
    insert_time::date = %s AND machine_name = %s
GROUP BY
    CASE WHEN end_time IS NULL THEN create_time::DATE ELSE end_time::DATE END,
    insert_time::DATE,
    machine_name,
    CASE WHEN global_user_name ISNULL
        THEN (machine_name || ':' || local_user_id)
        ELSE global_user_name
    END,
    vo_issuer,
    vo_name,
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


    @defer.inlineCallbacks
    def update(self):
        # will update the parts of the aggregated data table which has been
        # specified to need an update in the update table

        self.updating = True

        try:
            # first we get the info for what should be updated
            rows = yield self.dbpool.runQuery(UPDATE_INFO_QUERY)
            n_rows = len(rows)
            #print 'Updates: %i to be performed' % n_rows
            for row in rows:
                insert_date, machine_name = row
                insert_date = str(insert_date)

                yield self.dbpool.runOperation(DELETE_AGGREGATED_INFO, (insert_date, machine_name))
                yield self.dbpool.runOperation(UPDATE_AGGREGATED_INFO, (insert_date, machine_name))
                #stm = UPDATE_AGGREGATED_INFO.replace('\n', ' ').replace('  ', ' ')
                #yield self.dbpool.runOperation(stm, (insert_date, machine_name))

            yield self.dbpool.runOperation(TRUNCATE_UPDATE_TABLE)

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


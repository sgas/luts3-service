# view info stuff

import time

from twisted.python import log
from twisted.internet import defer
from twisted.application import service




class ChunkDeserializationError(Exception):
    """
    Raised if anything goes wrong during deserilization.
    """



def deserializeInfoChunk(row):

    # from the view definition
    # key = [date_,host,user,vo,vo_group,vo_role];
    # value = [cpu_time, wall_time];
    try:
        key = row['key']
        value = row['value']
        year, month, day = key[0].split(' ')

        ic = {
            'year'     : year,
            'month'    : month,
            'day'      : day,
            'host'     : key[1],
            'user'     : key[2],
            'vo'       : key[3],
            'vo_group' : key[4],
            'vo_role'  : key[5],
            'count'    : value[0],
            'walltime' : value[1],
            'cputime'  : value[2]
        }
        return ic
    except (TypeError, ValueError), e:
        raise ChunkDeserializationError(str(e))



def buildKeySets(chunks, attributes=None):

    if attributes is None:
        attributes = ['user', 'host', 'vo']

    key_sets = {}

    for entry in chunks:
        for attr in attributes:
            if attr in entry:
                key_sets.setdefault(attr, set()).add(entry[attr])

    return key_sets



class InformationChunkManager(service.Service):

    def __init__(self, db, design, view, UPDATE_THRESHOLD=300):
        self.db = db
        self.design = design
        self.view = view
        self.update_threshold = UPDATE_THRESHOLD

        self.chunks = None

        self.last_update = None
        self.last_issue  = None
        self.deferreds   = []


    def startService(self):
        self._fetchChunks()


    def stopService(self):
        if self.deferreds:
            return defer.DeferredList(self.deferreds)
        else:
            return defer.success(None)


    def getInformationChunks(self):

        def removeDeferred(result, d):
            self.deferreds.remove(d)
            return result

        if self.chunks is None:
            return self._fetchChunks()

        # issue new fetch if threshold has been passed, and no new fetch have been issued
        elif time.time() > self.last_update + self.update_threshold and self.last_issue < self.last_update:
            d = self._fetchChunks()
            self.deferreds.append(d)
            d.addCallback(removeDeferred)
            return self.chunks

        else:
            return defer.success(self.chunks)


    def _fetchChunks(self):

        # we need a timeout timer here

        def getRows(query_result):
            return query_result['rows']

        def deserializeRows(rows):
            chunks = []
            for row in rows:
                try:
                    chunks.append(deserializeInfoChunk(row))
                except ChunkDeserializationError, e:
                    log.msg("Error during info chunk deserialization")
                    log.error(e)
            return chunks

        def updateChunks(chunks):
            self.chunks = chunks
            self.last_update = time.time()
            return chunks

        d = self.db.queryView(self.design, self.view)
        d.addCallback(getRows)
        d.addCallback(deserializeRows)
        d.addCallback(updateChunks)

        self.last_issue = time.time()

        return d




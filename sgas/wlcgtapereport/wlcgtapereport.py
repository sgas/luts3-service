"""
A plugin to implement WLCG Tape reporting according to
https://twiki.cern.ch/twiki/bin/view/HEPTape/TapeMetricsJSON

Author: Erik Edelmann <edelmann@csc.fi>
Copyright: Nordic e-Infrastructure Collaboration (2019)
"""

from twisted.python import log
from twisted.web import resource, server

from sgas.ext.python import json
from sgas.server import resourceutil
import time


JSON_MIME_TYPE = 'application/json'
HTTP_HEADER_CONTENT_TYPE = 'content-type'

WLCG_VOs = ('alice', 'atlas', 'cms')

GIGA = 1000000000 # True SI Gigas!

query = """
select
    case
        when group_identity like 'atlas%' then 'atlas'
        else group_identity
    end as vo,
    sum(resource_capacity_used) as used_bytes,
    extract(epoch from insert_time) as updated
from storagerecords
where
    storage_media = 'tape' and
    insert_time = (select insert_time from storagerecords order by insert_time desc limit 1)
group by
    insert_time,
    vo
;
"""


def find_dcache_version():
    import subprocess

    curl = subprocess.Popen(['curl', '-k', '-v', 'https://dav.ndgf.org/'],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout,stderr = curl.communicate()
    for x in stdout.splitlines():
        xs = str(x, 'utf-8')
        if xs.startswith("< Server: dCache"):
            return xs.split('/')[1]
    return None


class WLCGTapeReport(resource.Resource):
    
    PLUGIN_ID   = 'wlcgtape'
    PLUGIN_NAME = 'WLCGTape'

    isLeaf = True

    def __init__(self, cfg, db, authorizer):
        resource.Resource.__init__(self)
        self.db = db

        self.dcache_version = None
        self.dcache_version_lastcheck = 0


    def queryDatabase(self):
        #return self.db.dictquery(query, query_args)
        return self.db.dictquery(query)


    def render_GET(self, request):
        hostname = resourceutil.getHostname(request)
        log.msg('Accepted query request from %s' % hostname, system='sgas.WLCGTapeReport')

        def gotDatabaseResult(rows):

            latestupdate = int(rows[0]['updated'])
            now = int(time.time())

            if not self.dcache_version or now - self.dcache_version_lastcheck > 24*3600:
                v = find_dcache_version()
                if v:
                    self.dcache_version = v
                    self.dcache_version_lastcheck = now
                else:
                    if self.dcache_version:
                        log.msg("WARNING: Didn't get dCache version information, will rely on old info (%s)" \
                                % self.dcache_version)
                    else:
                        log.msg("WARNING: Didn't get dCache version information, and no old info to use!")

            storageshares = []
            for r in rows:
                if r["vo"] not in WLCG_VOs: continue

                used_bytes_rounded = int(r["used_bytes"]/GIGA) * GIGA # In bytes, rounded to GBs
                ss = { "name": r["vo"].upper(),
                       "usedsize": used_bytes_rounded,
                       "occupiedsize": used_bytes_rounded,
                       "timestamp": now,
                       "vos": [ r["vo"] ] }
                storageshares.append(ss)

            report = {
                "storageservice": {
                    "name": "NDGF-T1-Tape",
                    "implementation": "DCACHE",
                    "implementationversion": self.dcache_version,
                    "latestupdate": latestupdate,
                    "storageshares": storageshares
                }
            }

            payload = json.dumps(report)
            request.setHeader(HTTP_HEADER_CONTENT_TYPE, JSON_MIME_TYPE)
            request.write(payload.encode('utf-8'))
            request.finish()

        def queryError(error):
            log.msg('Queryengine error: %s' % str(error), system='sgas.WLCGTapeReport')
            request.setResponseCode(500)
            request.write(('Queryengine error (%s)' % str(error)).encode('utf-8'))
            request.finish()

        def resultHandlingError(error):
            log.msg('Query result error: %s' % str(error), system='sgas.WLCGTapeReport')
            request.setResponseCode(500)
            request.write(('Query result error (%s)' % str(error)).encode('utf-8'))
            request.finish()

        d = self.queryDatabase()
        d.addCallbacks(gotDatabaseResult, queryError)
        d.addErrback(resultHandlingError)
        return server.NOT_DONE_YET


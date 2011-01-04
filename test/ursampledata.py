"""
Some sample usage records.
"""


UR1_ID = 'gsiftp://example.org/jobs/1'
UR1_MACHINE_NAME = 'benedict.grid.aau.dk'
UR1 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2009-08-09T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/1" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/1</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 1</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-08-07T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-08-07T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT234S</ur:WallDuration>
</ur:JobUsageRecord>
"""


UR2_ID = 'gsiftp://example.org/jobs/2'
UR2_MACHINE_NAME = 'benedict.grid.aau.dk'
UR2 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2009-09-10T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/2" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/2</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 2</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-09-10T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-09-10T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT345S</ur:WallDuration>
</ur:JobUsageRecord>
"""


CUR_IDS = [ "gsiftp://example.org/jobs/3", "gsiftp://example.org/jobs/4" ]
CUR = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:UsageRecords xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
  <ur:JobUsageRecord>
    <ur:RecordIdentity ur:createTime="2009-10-11T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/3" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/3</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 3</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-10-11T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-10-11T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT456S</ur:WallDuration>
  </ur:JobUsageRecord>
  <ur:JobUsageRecord>
    <ur:RecordIdentity ur:createTime="2009-12-13T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/4" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/4</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 4</ur:JobName>
    <ur:MachineName>fyrgrid.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-12-13T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-12-13T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT678S</ur:WallDuration>
  </ur:JobUsageRecord>
</ur:UsageRecords>
"""


URT_ID = "zalizo.uio.no:21305"
URT = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2010-10-22T14:51:18Z" ur:recordId="zalizo.uio.no:21305" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://test.ndgf.org:2811/jobs/1125412877590561135129704</ur:GlobalJobId>
        <ur:LocalJobId>21305</ur:LocalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:LocalUserId>sgastest</ur:LocalUserId>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
        <vo:VO vo:type="voms" xmlns:vo="http://www.sgas.se/namespaces/2009/05/ur/vo">
            <vo:Name>atlas</vo:Name>
            <vo:Issuer>/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch</vo:Issuer>
            <vo:Attribute>
                <vo:Group>atlas</vo:Group>
            </vo:Attribute>
            <vo:Attribute>
                <vo:Group>atlas/lcg1</vo:Group>
            </vo:Attribute>
            <vo:Attribute>
                <vo:Group>atlas/no</vo:Group>
            </vo:Attribute>
        </vo:VO>
    </ur:UserIdentity>
    <ur:Status>completed</ur:Status>
    <ur:MachineName>zalizo.uio.no</ur:MachineName>
    <ur:Queue>fork</ur:Queue>
    <ur:Host>zalizo.uio.no</ur:Host>
    <ur:NodeCount>1</ur:NodeCount>
    <ur:Processors>1</ur:Processors>
    <deisa:SubmitTime xmlns:deisa="http://rmis.deisa.org/acct">2010-10-22T14:50:56Z</deisa:SubmitTime>
    <ur:StartTime>2010-10-22T14:51:08Z</ur:StartTime>
    <ur:EndTime>2010-10-22T14:51:18Z</ur:EndTime>
    <ur:WallDuration>PT10S</ur:WallDuration>
    <ur:CpuDuration>PT0S</ur:CpuDuration>
    <sgas:UserTime xmlns:sgas="http://www.sgas.se/namespaces/2009/07/ur">PT0S</sgas:UserTime>
    <sgas:KernelTime xmlns:sgas="http://www.sgas.se/namespaces/2009/07/ur">PT0S</sgas:KernelTime>
    <sgas:ExitCode xmlns:sgas="http://www.sgas.se/namespaces/2009/07/ur">0</sgas:ExitCode>
    <sgas:MajorPageFaults xmlns:sgas="http://www.sgas.se/namespaces/2009/07/ur">0</sgas:MajorPageFaults>
    <logger:LoggerName logger:version="0.8.4" xmlns:logger="http://www.sgas.se/namespaces/2010/08/logger">ARC0-URLogger</logger:LoggerName>
    <ns1:FileTransfers xmlns:ns1="http://www.sgas.se/namespaces/2010/10/filetransfer">
        <ns1:FileDownload>
            <ns1:URL>srm://srm.ndgf.org/atlas/disk/atlaslocalgroupdisk/no/user10/testfile4</ns1:URL>
            <ns1:Size>10</ns1:Size>
            <ns1:StartTime>2010-10-22T16:50:57Z</ns1:StartTime>
            <ns1:EndTime>2010-10-22T16:50:59Z</ns1:EndTime>
            <ns1:BypassCache>False</ns1:BypassCache>
            <ns1:RetrievedFromCache>False</ns1:RetrievedFromCache>
        </ns1:FileDownload>
        <ns1:FileDownload>
            <ns1:URL>srm://srm.ndgf.org/atlas/disk/atlaslocalgroupdisk/no/user10/testfile</ns1:URL>
            <ns1:Size>5</ns1:Size>
            <ns1:StartTime>2010-10-22T16:50:57Z</ns1:StartTime>
            <ns1:EndTime>2010-10-22T16:51:00Z</ns1:EndTime>
            <ns1:BypassCache>True</ns1:BypassCache>
            <ns1:RetrievedFromCache>False</ns1:RetrievedFromCache>
        </ns1:FileDownload>
        <ns1:FileUpload>
            <ns1:URL>srm://srm.ndgf.org/atlas/disk/atlaslocalgroupdisk/no/user10/testfile42</ns1:URL>
            <ns1:Size>11</ns1:Size>
            <ns1:StartTime>2010-10-22T16:51:16Z</ns1:StartTime>
            <ns1:EndTime>2010-10-22T16:51:18Z</ns1:EndTime>
        </ns1:FileUpload>
        <ns1:FileUpload>
            <ns1:URL>srm://srm.ndgf.org/atlas/disk/atlaslocalgroupdisk/no/user10/testfile41</ns1:URL>
            <ns1:Size>10</ns1:Size>
            <ns1:StartTime>2010-10-22T16:51:16Z</ns1:StartTime>
            <ns1:EndTime>2010-10-22T16:51:18Z</ns1:EndTime>
        </ns1:FileUpload>
        <ns1:FileUpload>
            <ns1:URL>srm://srm.ndgf.org/atlas/disk/atlaslocalgroupdisk/no/user10/testfile43</ns1:URL>
            <ns1:Size>12</ns1:Size>
            <ns1:StartTime>2010-10-22T16:51:16Z</ns1:StartTime>
            <ns1:EndTime>2010-10-22T16:51:18Z</ns1:EndTime>
        </ns1:FileUpload>
    </ns1:FileTransfers>
</ur:JobUsageRecord>
"""


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
    <ur:WallDuration>PT234.1S</ur:WallDuration>
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


UR_BAD_LOCAL_JOB_ID_RECORD_ID = '''recordId="ce03.titan.uio.no:sbatch: error: Slurm temporarily unable to accept job, sleeping and retrying.'''
UR_BAD_LOCAL_JOB_ID_GLOBAL_JOB_ID = '''gsiftp://ce03.titan.uio.no:2811/jobs/981012957074611335880653'''
UR_BAD_LOCAL_JOB_ID = """
<ns0:JobUsageRecord xmlns:ns0="http://schema.ogf.org/urf/2003/09/urf">
    <ns0:RecordIdentity ns0:createTime="2011-01-24T12:45:48Z" ns0:recordId="ce03.titan.uio.no:sbatch: error: Slurm temporarily unable to accept job, sleeping and retrying." />
    <ns0:JobIdentity>
        <ns0:GlobalJobId>gsiftp://ce03.titan.uio.no:2811/jobs/981012957074611335880653</ns0:GlobalJobId>
        <ns0:LocalJobId>sbatch: error: Slurm temporarily unable to accept job, sleeping and retrying.</ns0:LocalJobId>
    </ns0:JobIdentity>
    <ns0:UserIdentity>
        <ns0:LocalUserId>grid</ns0:LocalUserId>
        <ns0:GlobalUserName>/C=SI/O=SiGNET/O=IJS/OU=F9/CN=Andrej Filipcic</ns0:GlobalUserName>
        <ns1:VO ns1:type="voms" xmlns:ns1="http://www.sgas.se/namespaces/2009/05/ur/vo">
            <ns1:Name>atlas</ns1:Name>
            <ns1:Issuer>/DC=ch/DC=cern/OU=computers/CN=lcg-voms.cern.ch</ns1:Issuer>
            <ns1:Attribute>
                <ns1:Group>atlas</ns1:Group>
                <ns1:Role>production</ns1:Role>
            </ns1:Attribute>
            <ns1:Attribute>
                <ns1:Group>atlas</ns1:Group>
            </ns1:Attribute>
            <ns1:Attribute>
                <ns1:Group>atlas/lcg1</ns1:Group>
            </ns1:Attribute>
        </ns1:VO>
    </ns0:UserIdentity>
    <ns0:JobName>mc10_7TeV.107700.AlpgenJimmyWtaunuNp0_pt20.simul.e600_s933_tid251613._063586.job</ns0:JobName>
    <ns0:Status>completed</ns0:Status>
    <ns0:MachineName>ce03.titan.uio.no</ns0:MachineName>
    <ns0:Queue>atlas-t1-reprocessing</ns0:Queue>
    <ns0:Processors>1</ns0:Processors>
    <ns1:SubmitTime xmlns:ns1="http://rmis.deisa.org/acct">2011-01-22T14:44:22Z</ns1:SubmitTime>
    <ns0:StartTime>2011-01-24T12:45:48Z</ns0:StartTime>
    <ns0:EndTime>2011-01-24T12:45:48Z</ns0:EndTime>
    <ns1:LoggerName ns1:version="0.8.3.1" xmlns:ns1="http://www.sgas.se/namespaces/2010/08/logger">ARC0-URLogger</ns1:LoggerName>
    <ns1:FileTransfers xmlns:ns1="http://www.sgas.se/namespaces/2010/10/filetransfer">
        <ns1:FileDownload>
            <ns1:URL>lfc://lfc1.ndgf.org//grid/atlas/dq2/mc10_7TeV/EVNT/mc10_7TeV.107700.AlpgenJimmyWtaunuNp0_pt20.evgen.EVNT.e600_tid251393_00/EVNT.251393._003662.pool.root.1</ns1:URL>
            <ns1:Size>12036413</ns1:Size>
            <ns1:StartTime>2011-01-22T14:44:24Z</ns1:StartTime>
            <ns1:EndTime>2011-01-22T14:44:26Z</ns1:EndTime>
            <ns1:RetrievedFromCache>true</ns1:RetrievedFromCache>
        </ns1:FileDownload>
        <ns1:FileDownload>
            <ns1:URL>lfc://lfc1.ndgf.org//grid/atlas/dq2/user/user.andrejfilipcic.production/pilot3-SULU44i.tgz</ns1:URL>
            <ns1:Size>267490</ns1:Size>
            <ns1:StartTime>2011-01-22T14:44:24Z</ns1:StartTime>
            <ns1:EndTime>2011-01-22T14:44:26Z</ns1:EndTime>
            <ns1:RetrievedFromCache>true</ns1:RetrievedFromCache>
        </ns1:FileDownload>
        <ns1:FileDownload>
            <ns1:URL>lfc:////grid/atlas/dq2/user/user.andrejfilipcic.production/NGpilot.8</ns1:URL>
            <ns1:Size>2439</ns1:Size>
            <ns1:StartTime>2011-01-22T14:44:24Z</ns1:StartTime>
            <ns1:EndTime>2011-01-22T14:44:26Z</ns1:EndTime>
            <ns1:RetrievedFromCache>true</ns1:RetrievedFromCache>
        </ns1:FileDownload>
        <ns1:FileDownload>
            <ns1:URL>lfc://lfc1.ndgf.org//grid/atlas/dq2/ddo/DBRelease/v120201/ddo.000001.Atlas.Ideal.DBRelease.v120201/DBRelease-12.2.1.tar.gz</ns1:URL>
            <ns1:Size>427086136</ns1:Size>
            <ns1:StartTime>2011-01-22T14:44:57Z</ns1:StartTime>
            <ns1:EndTime>2011-01-22T14:45:05Z</ns1:EndTime>
            <ns1:RetrievedFromCache>true</ns1:RetrievedFromCache>
        </ns1:FileDownload>
    </ns1:FileTransfers>
</ns0:JobUsageRecord>
"""


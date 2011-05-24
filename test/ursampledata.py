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
    <ur:Charge>7.4</ur:Charge>
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


UR_LONGHOST_ID = 'gsiftp://example.org/jobs/1'
UR_LONGHOST_MACHINE_NAME = 'benedict.grid.aau.dk'
UR_LONGHOST = """<?xml version="1.0" encoding="UTF-8" ?>
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
    <ur:Host>node1,node2,node3,node4,node5,node6,node7,node8,node9,node10,node11,node12,node13,node14,node15,node16,node17,node18,node19,node20,node21,node22,node23,node24,node25,node26,node27,node28,node29,node30,node31,node32,node33,node34,node35,node36,node37,node38,node39,node40,node41,node42,node43,node44,node45,node46,node47,node48,node49,node50,node51,node52,node53,node54,node55,node56,node57,node58,node59,node60,node61,node62,node63,node64,node65,node66,node67,node68,node69,node70,node71,node72,node73,node74,node75,node76,node77,node78,node79,node80,node81,node82,node83,node84,node85,node86,node87,node88,node89,node90,node91,node92,node93,node94,node95,node96,node97,node98,node99,node100,node101,node102,node103,node104,node105,node106,node107,node108,node109,node110,node111,node112,node113,node114,node115,node116,node117,node118,node119,node120,node121,node122,node123,node124,node125,node126,node127,node128,node129,node130,node131,node132,node133,node134,node135,node136,node137,node138,node139,node140,node141,node142,node143,node144,node145,node146,node147,node148,node149,node150,node151,node152,node153,node154,node155,node156,node157,node158,node159,node160,node161,node162,node163,node164,node165,node166,node167,node168,node169,node170,node171,node172,node173,node174,node175,node176,node177,node178,node179,node180,node181,node182,node183,node184,node185,node186,node187,node188,node189,node190,node191,node192,node193,node194,node195,node196,node197,node198,node199,node200,node201,node202,node203,node204,node205,node206,node207,node208,node209,node210,node211,node212,node213,node214,node215,node216,node217,node218,node219,node220,node221,node222,node223,node224,node225,node226,node227,node228,node229,node230,node231,node232,node233,node234,node235,node236,node237,node238,node239,node240,node241,node242,node243,node244,node245,node246,node247,node248,node249,node250,node251,node252,node253,node254,node255,node256,node257,node258,node259,node260,node261,node262,node263,node264,node265,node266,node267,node268,node269,node270,node271,node272,node273,node274,node275,node276,node277,node278,node279,node280,node281,node282,node283,node284,node285,node286,node287,node288,node289,node290,node291,node292,node293,node294,node295,node296,node297,node298,node299,node300,node301,node302,node303,node304,node305,node306,node307,node308,node309,node310,node311,node312,node313,node314,node315,node316,node317,node318,node319,node320,node321,node322,node323,node324,node325,node326,node327,node328,node329,node330,node331,node332,node333,node334,node335,node336,node337,node338,node339,node340,node341,node342,node343,node344,node345,node346,node347,node348,node349,node350,node351,node352,node353,node354,node355,node356,node357,node358,node359,node360,node361,node362,node363,node364,node365,node366,node367,node368,node369,node370,node371,node372,node373,node374,node375,node376,node377,node378,node379,node380,node381,node382,node383,node384,node385,node386,node387,node388,node389,node390,node391,node392,node393,node394,node395,node396,node397,node398,node399,node400</ur:Host>
    <ur:Charge>7.4</ur:Charge>
    <ur:StartTime>2009-08-07T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-08-07T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT234.1S</ur:WallDuration>
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



UR_BAD_EXIT_CODE_ID = "job.bad.exit.code"
UR_BAD_EXIT_CODE = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2010-10-22T14:51:18Z" ur:recordId="job.bad.exit.code" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://test.ndgf.org:2811/jobs/badexitcodejob12354</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:LocalUserId>sgastest</ur:LocalUserId>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
        <vo:VO vo:type="voms" xmlns:vo="http://www.sgas.se/namespaces/2009/05/ur/vo">
            <vo:Name>atlas</vo:Name>
        </vo:VO>
    </ur:UserIdentity>
    <ur:Status>completed</ur:Status>
    <ur:MachineName>zalizo.uio.no</ur:MachineName>
    <ur:Queue>fork</ur:Queue>
    <ur:NodeCount>1</ur:NodeCount>
    <ur:Processors>1</ur:Processors>
    <deisa:SubmitTime xmlns:deisa="http://rmis.deisa.org/acct">2010-10-22T14:50:56Z</deisa:SubmitTime>
    <ur:StartTime>2010-10-22T14:51:08Z</ur:StartTime>
    <ur:EndTime>2010-10-22T14:51:18Z</ur:EndTime>
    <ur:WallDuration>PT10S</ur:WallDuration>
    <ur:CpuDuration>PT0S</ur:CpuDuration>
    <sgas:ExitCode xmlns:sgas="http://www.sgas.se/namespaces/2009/07/ur">68774</sgas:ExitCode>
</ur:JobUsageRecord>
"""



"""
Some sample usage records.
"""


UR1 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/1" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/1</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 1</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-07T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-07T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT1S</ur:WallDuration>
</ur:JobUsageRecord>
"""


UR2 = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:JobUsageRecord xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/2" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/2</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 2</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-08T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-08T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT2S</ur:WallDuration>
</ur:JobUsageRecord>
"""


CUR = """<?xml version="1.0" encoding="UTF-8" ?>
<ur:UsageRecords xmlns:ur="http://schema.ogf.org/urf/2003/09/urf">
  <ur:JobUsageRecord>
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/3" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/3</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 3</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-08T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-08T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT3S</ur:WallDuration>
  </ur:JobUsageRecord>
  <ur:JobUsageRecord>
    <ur:RecordIdentity ur:createTime="2009-07-07T09:06:52Z" ur:recordId="gsiftp://example.org/jobs/4" />
    <ur:JobIdentity>
        <ur:GlobalJobId>gsiftp://example.org/jobs/4</ur:GlobalJobId>
    </ur:JobIdentity>
    <ur:UserIdentity>
        <ur:GlobalUserName>/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Test User</ur:GlobalUserName>
    </ur:UserIdentity>
    <ur:JobName>test job 4</ur:JobName>
    <ur:MachineName>benedict.grid.aau.dk</ur:MachineName>
    <ur:StartTime>2009-07-08T09:06:37Z</ur:StartTime>
    <ur:EndTime>2009-07-08T09:06:52Z</ur:EndTime>
    <ur:WallDuration>PT4S</ur:WallDuration>
  </ur:JobUsageRecord>
</ur:UsageRecords>
"""


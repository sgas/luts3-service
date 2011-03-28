"""
Some sample storage records.
"""

SR_0_ID = "host.example.org/sr/87912469269276"
SR_0 = """<sr:StorageUsageRecords xmlns:sr="http://eu-emi.eu/namespaces/2011/02/storagerecord">
  <sr:StorageUsageRecord>
  <sr:RecordIdentity sr:createTime="2010-11-09T09:06:52Z" sr:recordId="host.example.org/sr/87912469269276"/>
  <sr:StorageSystem>host.example.org</sr:StorageSystem>
  <sr:StorageShare>pool-003</sr:StorageShare>
  <sr:StorageMedia>disk</sr:StorageMedia>
  <sr:StorageClass>replicated</sr:StorageClass>
  <sr:FileCount>42</sr:FileCount>
  <sr:DirectoryPath>/home/projectA</sr:DirectoryPath>
  <sr:SubjectIdentity>
    <sr:LocalUser>johndoe</sr:LocalUser>
    <sr:LocalGroup>projectA</sr:LocalGroup>
    <sr:UserIdentity>/O=Grid/OU=example.org/CN=John Doe</sr:UserIdentity>
    <sr:Group>binarydataproject.example.org</sr:Group>
    <sr:GroupAttribute sr:attributeType="subgroup">ukusers</sr:GroupAttribute>
  </sr:SubjectIdentity>
  <sr:MeasureTime>2010-10-11T09:31:40Z</sr:MeasureTime>
  <sr:ValidDuration>PT3600S</sr:ValidDuration>
  <sr:ResourceCapacityUsed>14728</sr:ResourceCapacityUsed>
  <sr:LogicalCapacityUsed>13617</sr:LogicalCapacityUsed>
  </sr:StorageUsageRecord>
</sr:StorageUsageRecords>
"""

SR_1_ID = "442a98bc-5159-11e0-9c3b-001f1607034c"
SR_1 = """<?xml version="1.0" encoding="UTF-8" ?>
<ns0:StorageUsageRecord xmlns:ns0="http://eu-emi.eu/namespaces/2011/02/storagerecord">
   <ns0:RecordIdentity ns0:createTime="2011-03-18T12:14:25Z" ns0:recordId="442a98bc-5159-11e0-9c3b-001f1607034c" />
   <ns0:StorageSystem>dcache.example.org</ns0:StorageSystem>
   <ns0:SubjectIdentity>
     <ns0:Group>Alice</ns0:Group>
   </ns0:SubjectIdentity>
   <ns0:MeasureTime>2011-03-08T00:00:00Z</ns0:MeasureTime>
   <ns0:ResourceCapacityUsed>249</ns0:ResourceCapacityUsed>
</ns0:StorageUsageRecord>
"""

SR_2_ID = "host.example.org/sr/87912469269277"
SR_2 = """<sr:StorageUsageRecord xmlns:sr="http://eu-emi.eu/namespaces/2011/02/storagerecord">
  <sr:RecordIdentity sr:createTime="2010-11-09T09:06:52Z" sr:recordId="host.example.org/sr/87912469269277"/>
  <sr:StorageSystem>host.example.org</sr:StorageSystem>
  <sr:StorageShare>pool-003</sr:StorageShare>
  <sr:SubjectIdentity>
    <sr:Group>binarydataproject.example.org</sr:Group>
    <sr:GroupAttribute sr:attributeType="subgroup">ukusers</sr:GroupAttribute>
  </sr:SubjectIdentity>
  <sr:StorageMedia>disk</sr:StorageMedia>
  <sr:FileCount>42</sr:FileCount>
  <sr:MeasureTime>2010-10-11T09:31:40Z</sr:MeasureTime>
  <sr:ValidDuration>PT3600S</sr:ValidDuration>
  <sr:ResourceCapacityUsed>14728</sr:ResourceCapacityUsed>
  <sr:LogicalCapacityUsed>13617</sr:LogicalCapacityUsed>
</sr:StorageUsageRecord>
"""

SR_3_ID = "host.example.org/sr/87912469269278"
SR_3 = """<sr:StorageUsageRecord xmlns:sr="http://eu-emi.eu/namespaces/2011/02/storagerecord">
  <sr:RecordIdentity sr:createTime="2010-11-09T09:06:52Z" sr:recordId="host.example.org/sr/87912469269278"/>
  <sr:StorageSystem>host.example.org</sr:StorageSystem>
  <sr:SubjectIdentity>
    <sr:LocalUser>johndoe</sr:LocalUser>
  </sr:SubjectIdentity>
  <sr:StorageMedia>tape</sr:StorageMedia>
  <sr:FileCount>18</sr:FileCount>
  <sr:MeasureTime>2010-10-11T09:31:40Z</sr:MeasureTime>
  <sr:ValidDuration>PT3600S</sr:ValidDuration>
  <sr:ResourceCapacityUsed>913617</sr:ResourceCapacityUsed>
</sr:StorageUsageRecord>
"""

SR_4_ID = "host.example.org/sr/87912469269279"
SR_4 = """<sr:StorageUsageRecord xmlns:sr="http://eu-emi.eu/namespaces/2011/02/storagerecord">
  <sr:RecordIdentity sr:createTime="2010-11-09T09:06:52Z" sr:recordId="host.example.org/sr/87912469269279"/>
  <sr:StorageSystem>host.example.org</sr:StorageSystem>
  <sr:MeasureTime>2010-10-11T09:31:40Z</sr:MeasureTime>
  <sr:ValidDuration>PT3600S</sr:ValidDuration>
  <sr:ResourceCapacityUsed>13617</sr:ResourceCapacityUsed>
</sr:StorageUsageRecord>
"""

SRS_IDS = [ SR_2_ID, SR_3_ID, SR_4_ID ]
SRS = """<?xml version="1.0" encoding="UTF-8" ?>
<ns0:StorageUsageRecords xmlns:ns0="http://eu-emi.eu/namespaces/2011/02/storagerecord">""" + \
    SR_2 + "\n" + SR_3 + "\n" + SR_4 + \
"</ns0:StorageUsageRecords>"



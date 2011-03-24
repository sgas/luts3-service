"""
XML elements for EMI storage record specification.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: NorduNET / Nordic Data Grid Facility (2011)
"""


from xml.etree.cElementTree import QName


# namespace
SR_NAMESPACE = "http://eu-emi.eu/namespaces/2011/02/storagerecord"

# record identity and metadata
STORAGE_USAGE_RECORD    = QName("{%s}StorageUsageRecord"    % SR_NAMESPACE)
STORAGE_USAGE_RECORDS   = QName("{%s}StorageUsageRecords"   % SR_NAMESPACE)
RECORD_IDENTITY         = QName("{%s}RecordIdentity"        % SR_NAMESPACE)
RECORD_ID               = QName("{%s}recordId"              % SR_NAMESPACE)
CREATE_TIME             = QName("{%s}createTime"            % SR_NAMESPACE)

# storage system
STORAGE_SYSTEM          = QName("{%s}StorageSystem"         % SR_NAMESPACE)
STORAGE_SHARE           = QName("{%s}StorageShare"          % SR_NAMESPACE)
STORAGE_MEDIA           = QName("{%s}StorageMedia"          % SR_NAMESPACE)
STORAGE_CLASS           = QName("{%s}StorageClass"          % SR_NAMESPACE)

# aggregates
FILE_COUNT              = QName("{%s}FileCount"             % SR_NAMESPACE)
DIRECTORY_PATH          = QName("{%s}DirectoryPath"         % SR_NAMESPACE)

# identity
SUBJECT_IDENTITY        = QName("{%s}SubjectIdentity"       % SR_NAMESPACE)
LOCAL_USER              = QName("{%s}LocalUser"             % SR_NAMESPACE)
LOCAL_GROUP             = QName("{%s}LocalGroup"            % SR_NAMESPACE)
USER_IDENTITY           = QName("{%s}UserIdentity"          % SR_NAMESPACE)
GROUP                   = QName("{%s}Group"                 % SR_NAMESPACE)
GROUP_ATTRIBUTE         = QName("{%s}GroupAttribute"        % SR_NAMESPACE)
ATTRIBUTE_TYPE          = QName("{%s}attributeType"         % SR_NAMESPACE)

# recource consumption
MEASURE_TIME            = QName("{%s}MeasureTime"           % SR_NAMESPACE)
VALID_DURATION          = QName("{%s}ValidDuration"         % SR_NAMESPACE)
RESOURCE_CAPACITY_USED  = QName("{%s}ResourceCapacityUsed"  % SR_NAMESPACE)
LOGICAL_CAPACITY_USED   = QName("{%s}LogicalCapacityUsed"   % SR_NAMESPACE)


"""
XML elements for the usage record standard.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""


from xml.etree.cElementTree import QName


# ur namespaces and tag names
OGF_UR_NAMESPACE    = "http://schema.ogf.org/urf/2003/09/urf"
DEISA_NAMESPACE     = "http://rmis.deisa.org/acct"
SGAS_VO_NAMESPACE   = "http://www.sgas.se/namespaces/2009/05/ur/vo"
SGAS_UR_NAMESPACE   = "http://www.sgas.se/namespaces/2009/07/ur"
LOGGER_NAMESPACE    = "http://www.sgas.se/namespaces/2010/08/logger"
TRANSFER_NAMESPACE  = "http://www.sgas.se/namespaces/2010/10/filetransfer"

# usage record tag names
USAGE_RECORDS       = QName("{%s}UsageRecords"   % OGF_UR_NAMESPACE)
JOB_USAGE_RECORD    = QName("{%s}JobUsageRecord" % OGF_UR_NAMESPACE)
RECORD_IDENTITY     = QName("{%s}RecordIdentity" % OGF_UR_NAMESPACE)
RECORD_ID           = QName("{%s}recordId"       % OGF_UR_NAMESPACE)
CREATE_TIME         = QName("{%s}createTime"     % OGF_UR_NAMESPACE)
JOB_IDENTITY        = QName("{%s}JobIdentity"    % OGF_UR_NAMESPACE)
GLOBAL_JOB_ID       = QName("{%s}GlobalJobId"    % OGF_UR_NAMESPACE)
LOCAL_JOB_ID        = QName("{%s}LocalJobId"     % OGF_UR_NAMESPACE)
USER_IDENTITY       = QName("{%s}UserIdentity"   % OGF_UR_NAMESPACE)
LOCAL_USER_ID       = QName("{%s}LocalUserId"    % OGF_UR_NAMESPACE)
GLOBAL_USER_NAME    = QName("{%s}GlobalUserName" % OGF_UR_NAMESPACE)
JOB_NAME            = QName("{%s}JobName"        % OGF_UR_NAMESPACE)
STATUS              = QName("{%s}Status"         % OGF_UR_NAMESPACE)
CHARGE              = QName("{%s}Charge"         % OGF_UR_NAMESPACE)
WALL_DURATION       = QName("{%s}WallDuration"   % OGF_UR_NAMESPACE)
CPU_DURATION        = QName("{%s}CpuDuration"    % OGF_UR_NAMESPACE)
NODE_COUNT          = QName("{%s}NodeCount"      % OGF_UR_NAMESPACE)
PROCESSORS          = QName("{%s}Processors"     % OGF_UR_NAMESPACE)
START_TIME          = QName("{%s}StartTime"      % OGF_UR_NAMESPACE)
END_TIME            = QName("{%s}EndTime"        % OGF_UR_NAMESPACE)
PROJECT_NAME        = QName("{%s}ProjectName"    % OGF_UR_NAMESPACE)
SUBMIT_HOST         = QName("{%s}SubmitHost"     % OGF_UR_NAMESPACE)
MACHINE_NAME        = QName("{%s}MachineName"    % OGF_UR_NAMESPACE)
HOST                = QName("{%s}Host"           % OGF_UR_NAMESPACE)
QUEUE               = QName("{%s}Queue"          % OGF_UR_NAMESPACE)


# third party tag names from here on

# sgas vo extensions
VO                  = QName("{%s}VO"             % SGAS_VO_NAMESPACE)
VO_TYPE             = QName("{%s}type"           % SGAS_VO_NAMESPACE)
VO_NAME             = QName("{%s}Name"           % SGAS_VO_NAMESPACE)
VO_ISSUER           = QName("{%s}Issuer"         % SGAS_VO_NAMESPACE)
VO_ATTRIBUTE        = QName("{%s}Attribute"      % SGAS_VO_NAMESPACE)
VO_GROUP            = QName("{%s}Group"          % SGAS_VO_NAMESPACE)
VO_ROLE             = QName("{%s}Role"           % SGAS_VO_NAMESPACE)
VO_CAPABILITY       = QName("{%s}Capability"     % SGAS_VO_NAMESPACE)

# deisa submit time extension
SUBMIT_TIME         = QName("{%s}SubmitTime"     % DEISA_NAMESPACE)

# sgas auxillary extensions
INSERT_TIME         = QName("{%s}insertTime"         % SGAS_UR_NAMESPACE)
KSI2K_WALL_DURATION = QName("{%s}KSI2KWallDuration"  % SGAS_UR_NAMESPACE)
KSI2K_CPU_DURATION  = QName("{%s}KSI2KCpuDuration"   % SGAS_UR_NAMESPACE)
USER_TIME           = QName("{%s}UserTime"           % SGAS_UR_NAMESPACE)
KERNEL_TIME         = QName("{%s}KernelTime"         % SGAS_UR_NAMESPACE)
EXIT_CODE           = QName("{%s}ExitCode"           % SGAS_UR_NAMESPACE)
MAJOR_PAGE_FAULTS   = QName("{%s}MajorPageFaults"    % SGAS_UR_NAMESPACE)
RUNTIME_ENVIRONMENT = QName("{%s}RuntimeEnvironment" % SGAS_UR_NAMESPACE)

# logger elements and attributes
LOGGER_NAME         = QName("{%s}LoggerName"         % LOGGER_NAMESPACE)
LOGGER_VERSION      = QName("{%s}version"            % LOGGER_NAMESPACE)

# elements for transfer stats
FILE_TRANSFERS                  = QName("{%s}FileTransfers"      % TRANSFER_NAMESPACE)
FILE_DOWNLOAD                   = QName("{%s}FileDownload"       % TRANSFER_NAMESPACE)
FILE_UPLOAD                     = QName("{%s}FileUpload"         % TRANSFER_NAMESPACE)
TRANSFER_URL                    = QName("{%s}URL"                % TRANSFER_NAMESPACE)
TRANSFER_SIZE                   = QName("{%s}Size"               % TRANSFER_NAMESPACE)
TRANSFER_START_TIME             = QName("{%s}StartTime"          % TRANSFER_NAMESPACE)
TRANSFER_END_TIME               = QName("{%s}EndTime"            % TRANSFER_NAMESPACE)
TRANSFER_BYPASS_CACHE           = QName("{%s}BypassCache"        % TRANSFER_NAMESPACE)
TRANSFER_RETRIEVED_FROM_CACHE   = QName("{%s}RetrievedFromCache" % TRANSFER_NAMESPACE)


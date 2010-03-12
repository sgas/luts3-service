"""
High level database access which incorportes usage records.
"""

import time
import hashlib
from xml.etree import cElementTree as ET

from twisted.python import log

from sgas.common import baseconvert
from sgas.ext import isodate


# ur namespaces and tag names
OGF_UR_NAMESPACE  = "http://schema.ogf.org/urf/2003/09/urf"
DEISA_NAMESPACE   = "http://rmis.deisa.org/acct"
SGAS_VO_NAMESPACE = "http://www.sgas.se/namespaces/2009/05/ur/vo"
SGAS_UR_NAMESPACE = "http://www.sgas.se/namespaces/2009/07/ur";

USAGE_RECORDS       = ET.QName("{%s}UsageRecords"   % OGF_UR_NAMESPACE)
JOB_USAGE_RECORD    = ET.QName("{%s}JobUsageRecord" % OGF_UR_NAMESPACE)
RECORD_IDENTITY     = ET.QName("{%s}RecordIdentity" % OGF_UR_NAMESPACE)
RECORD_ID           = ET.QName("{%s}recordId"       % OGF_UR_NAMESPACE)
CREATE_TIME         = ET.QName("{%s}createTime"     % OGF_UR_NAMESPACE)
JOB_IDENTITY        = ET.QName("{%s}JobIdentity"    % OGF_UR_NAMESPACE)
GLOBAL_JOB_ID       = ET.QName("{%s}GlobalJobId"    % OGF_UR_NAMESPACE)
LOCAL_JOB_ID        = ET.QName("{%s}LocalJobId"     % OGF_UR_NAMESPACE)
USER_IDENTITY       = ET.QName("{%s}UserIdentity"   % OGF_UR_NAMESPACE)
LOCAL_USER_ID       = ET.QName("{%s}LocalUserId"    % OGF_UR_NAMESPACE)
GLOBAL_USER_NAME    = ET.QName("{%s}GlobalUserName" % OGF_UR_NAMESPACE)
JOB_NAME            = ET.QName("{%s}JobName"        % OGF_UR_NAMESPACE)
STATUS              = ET.QName("{%s}Status"         % OGF_UR_NAMESPACE)
CHARGE              = ET.QName("{%s}Charge"         % OGF_UR_NAMESPACE)
WALL_DURATION       = ET.QName("{%s}WallDuration"   % OGF_UR_NAMESPACE)
CPU_DURATION        = ET.QName("{%s}CpuDuration"    % OGF_UR_NAMESPACE)
NODE_COUNT          = ET.QName("{%s}NodeCount"      % OGF_UR_NAMESPACE)
START_TIME          = ET.QName("{%s}StartTime"      % OGF_UR_NAMESPACE)
END_TIME            = ET.QName("{%s}EndTime"        % OGF_UR_NAMESPACE)
PROJECT_NAME        = ET.QName("{%s}ProjectName"    % OGF_UR_NAMESPACE)
SUBMIT_HOST         = ET.QName("{%s}SubmitHost"     % OGF_UR_NAMESPACE)
MACHINE_NAME        = ET.QName("{%s}MachineName"    % OGF_UR_NAMESPACE)
HOST                = ET.QName("{%s}Host"           % OGF_UR_NAMESPACE)
QUEUE               = ET.QName("{%s}Queue"          % OGF_UR_NAMESPACE)

VO                  = ET.QName("{%s}VO"             % SGAS_VO_NAMESPACE)
VO_TYPE             = ET.QName("{%s}type"           % SGAS_VO_NAMESPACE)
VO_NAME             = ET.QName("{%s}Name"           % SGAS_VO_NAMESPACE)
VO_ISSUER           = ET.QName("{%s}Issuer"         % SGAS_VO_NAMESPACE)
VO_ATTRIBUTE        = ET.QName("{%s}Attribute"      % SGAS_VO_NAMESPACE)
VO_GROUP            = ET.QName("{%s}Group"          % SGAS_VO_NAMESPACE)
VO_ROLE             = ET.QName("{%s}Role"           % SGAS_VO_NAMESPACE)
VO_CAPABILITY       = ET.QName("{%s}Capability"     % SGAS_VO_NAMESPACE)

SUBMIT_TIME         = ET.QName("{%s}SubmitTime"     % DEISA_NAMESPACE)

INSERT_TIME         = ET.QName("{%s}insertTime"         % SGAS_UR_NAMESPACE)
KSI2K_WALL_DURATION = ET.QName("{%s}KSI2KWallDuration"  % SGAS_UR_NAMESPACE)
KSI2K_CPU_DURATION  = ET.QName("{%s}KSI2KCpuDuration"   % SGAS_UR_NAMESPACE)
USER_TIME           = ET.QName("{%s}UserTime"           % SGAS_UR_NAMESPACE)
KERNEL_TIME         = ET.QName("{%s}KernelTime"         % SGAS_UR_NAMESPACE)
EXIT_CODE           = ET.QName("{%s}ExitCode"           % SGAS_UR_NAMESPACE)
MAJOR_PAGE_FAULTS   = ET.QName("{%s}MajorPageFaults"    % SGAS_UR_NAMESPACE)
RUNTIME_ENVIRONMENT = ET.QName("{%s}RuntimeEnvironment" % SGAS_UR_NAMESPACE)

# other constants
ISO_TIME_FORMAT   = "%Y-%m-%dT%H:%M:%SZ" # if we want to convert back some time
JSON_DATETIME_FORMAT = "%Y %m %d %H:%M:%S"


# new-style hash, produces smaller/faster b-trees in couchdb
def _create12byteb62hash(record_id):
    sha_160_hex = hashlib.sha1(record_id).hexdigest()
    sha_160 = int(sha_160_hex, 16)
    b62_12byte_max_length = 62**12
    b62_hash = baseconvert.base10to62(sha_160 % b62_12byte_max_length)
    return b62_hash


# old-style hash, no longer used
def _createSHA224Hash(record_id):
    return hashlib.sha224(record_id).hexdigest()


def createID(record_id):
    hash_id = _create12byteb62hash(record_id)
    return hash_id


def parseInt(value):
    try:
        return int(value)
    except ValueError:
        log.msg("Failed to parse float: %s" % value, system='sgas.usagerecord')
        return None

def parseFloat(value):
    try:
        return float(value)
    except ValueError:
        log.msg("Failed to parse float: %s" % value, system='sgas.usagerecord')
        return None


def parseISODuration(value):
    try:
        td = isodate.parse_duration(value)
        return (td.days * 3600*24) + td.seconds # screw microseconds
    except ValueError:
        log.msg("Failed to parse duration: %s" % value, system='sgas.usagerecord')
        return None


def parseISODateTime(value):
    try:
        dt = isodate.parse_datetime(value)
        return time.strftime(JSON_DATETIME_FORMAT, dt.utctimetuple())
    except ValueError, e:
        log.msg("Failed to parse datetime value: %s (%s)" % (value, str(e)), system='sgas.usagerecord')
        return None
    except isodate.ISO8601Error, e:
        log.msg("Failed to parse ISO datetime value: %s (%s)" % (value, str(e)), system='sgas.usagerecord')
        return None


def xmlToDict(ur, insert_identity=None, insert_hostname=None):
    # transfer a usage record xml element to a dictionary
    assert ur.tag == JOB_USAGE_RECORD

    r = {}

    def setIfNotNone(key, value):
        if key is not None:
            r[key] = value

    # set document type
    r['type'] = 'usagerecord'
    # version control for the convertion in sgas
    # prior to rc3 there was no such thing, but these are supposed to be version 1
    #
    # convert-version = 2 -- sgas 3.0 rc3
    # The datetime element are properly converted to UTC time and a format which can be parsed
    # by Date.parse in javascript.
    # Usually rc2 and prior versions of datetime can be converted by s/[TZ-]// on the string.
    # However rc2 could really be any form of iso datetime (which was the problem)
    #
    # conver-version = 4 -- sgas 3.1.0
    # use shorted _id field to save b-tree space in couchdb (faster views)
    #
    # the convert-version should be bumped when any changes to the json UR format are made.
    r['convert_version'] = 4

    r['insert_time'] = time.strftime(JSON_DATETIME_FORMAT, time.gmtime())
    if insert_identity is not None:
        r['insert_identity'] = insert_identity
    if insert_hostname is not None:
        r['insert_hostname'] = insert_hostname

    for element in ur:
        #print element
        if element.tag == RECORD_IDENTITY:
            setIfNotNone('_id',         createID(element.get(RECORD_ID)))
            setIfNotNone('record_id',   element.get(RECORD_ID))
            setIfNotNone('create_time', parseISODateTime(element.get(CREATE_TIME)))
        elif element.tag == JOB_IDENTITY:
            for subele in element:
                if    subele.tag == GLOBAL_JOB_ID:  r['global_job_id'] = subele.text
                elif  subele.tag == LOCAL_JOB_ID:   r['local_job_id']  = subele.text
                else: print "Unhandled job id element:", subele.tag

        elif element.tag == USER_IDENTITY:
            for subele in element:
                if    subele.tag == LOCAL_USER_ID:    r['local_user_id']    = subele.text
                elif  subele.tag == GLOBAL_USER_NAME: r['global_user_name'] = subele.text
                elif  subele.tag == VO:
                    setIfNotNone('vo_type', subele.get(VO_TYPE))
                    vo_attrs = []
                    for ve in subele:
                        if   ve.tag == VO_NAME:   r['vo_name'] = ve.text
                        elif ve.tag == VO_ISSUER: r['vo_issuer'] = ve.text
                        elif ve.tag == VO_ATTRIBUTE:
                            attr = {}
                            for va in ve:
                                if va.tag == VO_GROUP:
                                    attr['group'] = va.text
                                elif va.tag == VO_ROLE:
                                    attr['role'] = va.text
                                else:
                                    print "Unhandladed vo attribute element", va.tag
                            vo_attrs.append(attr)
                        else:
                            print "Unhandled vo subelement", ve.tag
                    if vo_attrs:
                        r['vo_attrs'] = vo_attrs
                else: print "Unhandled user id element:", subele.tag

        elif element.tag == JOB_NAME:       r['job_name']       = element.text
        elif element.tag == STATUS:         r['status']         = element.text
        elif element.tag == CHARGE:         r['charge']         = parseFloat(element.text)
        elif element.tag == WALL_DURATION:  r['wall_duration']  = parseISODuration(element.text)
        elif element.tag == CPU_DURATION:   r['cpu_duration']   = parseISODuration(element.text)
        elif element.tag == NODE_COUNT:     r['node_count']     = parseInt(element.text)
        elif element.tag == START_TIME:     r['start_time']     = parseISODateTime(element.text)
        elif element.tag == END_TIME:       r['end_time']       = parseISODateTime(element.text)
        elif element.tag == PROJECT_NAME:   r['project_name']   = element.text
        elif element.tag == SUBMIT_HOST:    r['submit_host']    = element.text
        elif element.tag == MACHINE_NAME:   r['machine_name']   = element.text
        elif element.tag == HOST:           r['host']           = element.text
        elif element.tag == QUEUE:          r['queue']          = element.text

        elif element.tag == SUBMIT_TIME:         r['submit_time']    = parseISODateTime(element.text)

        elif element.tag == KSI2K_WALL_DURATION: r['ksi2k_wall_duration'] = parseISODuration(element.text)
        elif element.tag == KSI2K_CPU_DURATION:  r['ksi2k_cpu_duration']  = parseISODuration(element.text)
        elif element.tag == USER_TIME:           r['user_time']           = parseISODuration(element.text)
        elif element.tag == KERNEL_TIME:         r['kernel_time']         = parseISODuration(element.text)
        elif element.tag == EXIT_CODE:           r['exit_code']           = parseInt(element.text)
        elif element.tag == MAJOR_PAGE_FAULTS:   r['major_page_faults']   = parseInt(element.text)
        elif element.tag == RUNTIME_ENVIRONMENT: r.setdefault('runtime_environments', []).append(element.text)

        else: print "Unhandled element:", element.tag

    return r



class ParseError(Exception):
    pass



class UsageRecordParser:


    def _getRecordId(self, ur):
        for ele in ur:
            if ele.tag == RECORD_IDENTITY:
                record_id = ele.get(RECORD_ID)
                if record_id is not None:
                    return record_id
        raise ParseError('Usage record within recordId found')


    def _addInsertTime(self, ur, timestamp):
        pass


    def getURDocuments(self, ur_data):

        urs = {}
        try:
            tree  = ET.fromstring(ur_data)
        except Exception, e:
            raise ParseError("Error parsing ur document (%s)" % str(e))

        if tree.tag == USAGE_RECORDS:
            for ur in tree:
                if not ur.tag == JOB_USAGE_RECORD:
                    raise ParseError("Subelement in UsageRecords doc not a JobUsageRecord")
                doc_id = self._getRecordId(ur)
                urs[doc_id] = ur

        elif tree.tag == JOB_USAGE_RECORD:
            doc_id = self._getRecordId(tree)
            urs[doc_id] = tree

        else:
            raise ParseError("Top element not a UsageRecords or JobUsageRecord")

        return urs


"""
Usage Record parser to convert Usage Records in XML format into Python
dictionaries, which are easier to work with.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import time

from twisted.python import log

from sgas.ext import isodate
from sgas.usagerecord import urelements as ur
from sgas.usagerecord.memory import SgasMemory


# date constants
ISO_TIME_FORMAT   = "%Y-%m-%dT%H:%M:%SZ" # if we want to convert back some time
JSON_DATETIME_FORMAT = "%Y %m %d %H:%M:%S"



def parseBoolean(value):
    if value == '1' or value.lower() == 'true':
        return True
    elif value == '0' or value.lower() == 'false':
        return False
    else:
        log.msg('Failed to parse value %s into boolean' % value, system='sgas.UsageRecord')
        return None


def parseInt(value):
    try:
        return int(value)
    except ValueError:
        log.msg("Failed to parse float: %s" % value, system='sgas.UsageRecord')
        return None


def parseFloat(value):
    try:
        return float(value)
    except ValueError:
        log.msg("Failed to parse float: %s" % value, system='sgas.UsageRecord')
        return None


def parseISODuration(value):
    try:
        td = isodate.parse_duration(value)
        return (td.days * 3600*24) + td.seconds # screw microseconds
    except ValueError:
        log.msg("Failed to parse duration: %s" % value, system='sgas.UsageRecord')
        return None


def parseISODateTime(value):
    try:
        dt = isodate.parse_datetime(value)
        return time.strftime(JSON_DATETIME_FORMAT, dt.utctimetuple())
    except ValueError, e:
        log.msg("Failed to parse datetime value: %s (%s)" % (value, str(e)), system='sgas.UsageRecord')
        return None
    except isodate.ISO8601Error, e:
        log.msg("Failed to parse ISO datetime value: %s (%s)" % (value, str(e)), system='sgas.UsageRecord')
        return None


def xmlToDict(ur_doc, insert_identity=None, insert_hostname=None, insert_time=None):
    # convert a usage record xml element into a dictionaries
    # only works for one ur element - use the ursplitter module to split
    # a ur document into seperate elements
    assert ur_doc.tag == ur.JOB_USAGE_RECORD

    r = {}

    def setIfNotNone(key, value):
        if key is not None:
            r[key] = value

    if insert_identity is not None:
        r['insert_identity'] = insert_identity
    if insert_hostname is not None:
        r['insert_hostname'] = insert_hostname
    if insert_time is not None:
        r['insert_time'] = time.strftime(JSON_DATETIME_FORMAT, insert_time)

    for element in ur_doc:
        if element.tag == ur.RECORD_IDENTITY:
            setIfNotNone('record_id',   element.get(ur.RECORD_ID))
            setIfNotNone('create_time', parseISODateTime(element.get(ur.CREATE_TIME)))
        elif element.tag == ur.JOB_IDENTITY:
            for subele in element:
                if    subele.tag == ur.GLOBAL_JOB_ID:  r['global_job_id'] = subele.text
                elif  subele.tag == ur.LOCAL_JOB_ID:   r['local_job_id']  = subele.text
                else: print("Unhandled job id element:", subele.tag)

        elif element.tag == ur.USER_IDENTITY:
            for subele in element:
                if    subele.tag == ur.LOCAL_USER_ID:    r['local_user_id']    = subele.text
                elif  subele.tag == ur.GLOBAL_USER_NAME: r['global_user_name'] = subele.text
                elif  subele.tag == ur.VO:
                    setIfNotNone('vo_type', subele.get(ur.VO_TYPE))
                    vo_attrs = []
                    for ve in subele:
                        if   ve.tag == ur.VO_NAME:   r['vo_name'] = ve.text
                        elif ve.tag == ur.VO_ISSUER: r['vo_issuer'] = ve.text
                        elif ve.tag == ur.VO_ATTRIBUTE:
                            attr = {}
                            for va in ve:
                                if va.tag == ur.VO_GROUP:
                                    attr['group'] = va.text
                                elif va.tag == ur.VO_ROLE:
                                    attr['role'] = va.text
                                else:
                                    print("Unhandladed vo attribute element", va.tag)
                            vo_attrs.append(attr)
                        else:
                            print("Unhandled vo subelement", ve.tag)
                    if vo_attrs:
                        r['vo_attrs'] = vo_attrs
                else: print("Unhandled user id element:", subele.tag)

        elif element.tag == ur.JOB_NAME:       r['job_name']       = element.text
        elif element.tag == ur.STATUS:         r['status']         = element.text
        elif element.tag == ur.CHARGE:         r['charge']         = parseFloat(element.text)
        elif element.tag == ur.WALL_DURATION:  r['wall_duration']  = parseISODuration(element.text)
        elif element.tag == ur.CPU_DURATION:   
            if 'cpu_duration' in r:
                r['cpu_duration'] += parseISODuration(element.text)
            else:
                r['cpu_duration'] = parseISODuration(element.text)
        elif element.tag == ur.NODE_COUNT:     r['node_count']     = parseInt(element.text)
        elif element.tag == ur.PROCESSORS:     r['processors']     = parseInt(element.text)
        elif element.tag == ur.START_TIME:     r['start_time']     = parseISODateTime(element.text)
        elif element.tag == ur.END_TIME:       r['end_time']       = parseISODateTime(element.text)
        elif element.tag == ur.PROJECT_NAME:   r['project_name']   = element.text
        elif element.tag == ur.SUBMIT_HOST:    r['submit_host']    = element.text
        elif element.tag == ur.MACHINE_NAME:   r['machine_name']   = element.text
        elif element.tag == ur.HOST:           r['host']           = element.text
        elif element.tag == ur.QUEUE:          r['queue']          = element.text

        elif element.tag == ur.SUBMIT_TIME:         r['submit_time']    = parseISODateTime(element.text)

        elif element.tag == ur.KSI2K_WALL_DURATION: log.msg('Got ksi2k wall duration element, ignoring (deprecated)', system='sgas.UsageRecord')
        elif element.tag == ur.KSI2K_CPU_DURATION:  log.msg('Got ksi2k cpu duration element, ignoring (deprecated)', system='sgas.UsageRecord')
        elif element.tag == ur.USER_TIME:           r['user_time']           = parseISODuration(element.text)
        elif element.tag == ur.KERNEL_TIME:         r['kernel_time']         = parseISODuration(element.text)
        elif element.tag == ur.EXIT_CODE:           r['exit_code']           = parseInt(element.text)
        elif element.tag == ur.MAJOR_PAGE_FAULTS:   r['major_page_faults']   = parseInt(element.text)
        elif element.tag == ur.SGAS_RUNTIME_ENVIRONMENT: r.setdefault('runtime_environments', []).append(element.text)
        elif element.tag == ur.ARC_RUNTIME_ENVIRONMENT: r.setdefault('runtime_environments', []).append(element.text)
        elif element.tag == ur.MEMORY:
            if not 'memory' in r:
                r['memory'] = []
            r['memory'].append(SgasMemory(parseInt(element.text),element.attrib.get(ur.MEMORY_STORAGE_UNIT),element.attrib.get(ur.MEMORY_METRIC),element.attrib.get(ur.MEMORY_TYPE)))
        elif element.tag == ur.LOGGER_NAME:
            pass # in the future this can be used to implement special handling for broken loggers, etc

        # transfer parse block
        elif element.tag == ur.FILE_TRANSFERS:
            for subele in element:
                if subele.tag == ur.FILE_DOWNLOAD:
                    download = {}
                    for dlele in subele:
                        if   dlele.tag == ur.TRANSFER_URL:
                            download['url'] = dlele.text
                        elif dlele.tag == ur.TRANSFER_SIZE:
                            download['size'] = parseInt(dlele.text)
                        elif dlele.tag == ur.TRANSFER_START_TIME:
                            download['start_time'] = parseISODateTime(dlele.text)
                        elif dlele.tag == ur.TRANSFER_END_TIME:
                            download['end_time'] = parseISODateTime(dlele.text)
                        elif dlele.tag == ur.TRANSFER_BYPASS_CACHE:
                            download['bypass_cache'] = parseBoolean(dlele.text)
                        elif dlele.tag == ur.TRANSFER_RETRIEVED_FROM_CACHE:
                            download['from_cache'] = parseBoolean(dlele.text)
                    r.setdefault('downloads', []).append(download)

                elif subele.tag == ur.FILE_UPLOAD:
                    upload = {}
                    for ulele in subele:
                        if   ulele.tag == ur.TRANSFER_URL:
                            upload['url'] = ulele.text
                        elif ulele.tag == ur.TRANSFER_SIZE:
                            upload['size'] = parseInt(ulele.text)
                        elif ulele.tag == ur.TRANSFER_START_TIME:
                            upload['start_time'] = parseISODateTime(ulele.text)
                        elif ulele.tag == ur.TRANSFER_END_TIME:
                            upload['end_time'] = parseISODateTime(ulele.text)
                        elif ulele.tag == ur.TRANSFER_BYPASS_CACHE:
                            upload['bypass_cache'] = parseBoolean(ulele.text)
                        elif ulele.tag == ur.TRANSFER_RETRIEVED_FROM_CACHE:
                            upload['from_cache'] = parseBoolean(ulele.text)
                    r.setdefault('uploads', []).append(upload)

        #else:
        #    log.msg("Unhandled UR element: %s" % element.tag, system='sgas.UsageRecord')

    # backwards logger compatability
    # alot of loggers set node_count when they should have used processors, therefore:
    # if node_count is set, but processors is not, processors is set to the value of node_count
    # the node_count value is reset.
    # If both values are set (which they should with updated loggers) nothing is done
    if 'processors' not in r and 'node_count' in r:
        r['processors'] = r['node_count']
        del r['node_count']

    return r


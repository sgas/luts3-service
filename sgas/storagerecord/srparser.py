"""
Parser to convert storage usage records in XML format into Python dictionaries,
which are easier to work with.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: NorduNET / Nordic Data Grid Facility (2011)
"""

import time

from twisted.python import log

from sgas.ext import isodate
from sgas.storagerecord import srelements as sr


# date constants
ISO_TIME_FORMAT   = "%Y-%m-%dT%H:%M:%SZ" # if we want to convert back some time
JSON_DATETIME_FORMAT = "%Y %m %d %H:%M:%S"


# These should be moved an independant module for reuse


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
    except ValueError as e:
        log.msg("Failed to parse datetime value: %s (%s)" % (value, str(e)), system='sgas.UsageRecord')
        return None
    except isodate.ISO8601Error as e:
        log.msg("Failed to parse ISO datetime value: %s (%s)" % (value, str(e)), system='sgas.UsageRecord')
        return None


# ---


def xmlToDict(sr_doc, insert_identity=None, insert_hostname=None, insert_time=None):
    # Convert a storage usage record xml element into a dictionaries
    # only works for one storage record element
    # Use the ursplitter module to split a sr document into seperate elements

    assert sr_doc.tag == sr.STORAGE_USAGE_RECORD

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


    for element in sr_doc:

        if element.tag == sr.RECORD_IDENTITY:
            setIfNotNone('record_id',   element.get(sr.RECORD_ID))
            setIfNotNone('create_time', parseISODateTime(element.get(sr.CREATE_TIME)))

        elif element.tag == sr.STORAGE_SYSTEM:          r['storage_system'] = element.text
        elif element.tag == sr.STORAGE_SHARE:           r['storage_share']  = element.text
        elif element.tag == sr.STORAGE_MEDIA:           r['storage_media']  = element.text
        elif element.tag == sr.STORAGE_CLASS:           r['storage_class']  = element.text
        elif element.tag == sr.SITE:                    r['site']           = element.text

        elif element.tag == sr.FILE_COUNT:              r['file_count']     = parseInt(element.text)
        elif element.tag == sr.DIRECTORY_PATH:          r['directory_path'] = element.text

        elif element.tag == sr.SUBJECT_IDENTITY:
            for subele in element:
                if   subele.tag == sr.LOCAL_USER:       r['local_user']     = subele.text
                elif subele.tag == sr.LOCAL_GROUP:      r['local_group']    = subele.text
                elif subele.tag == sr.USER_IDENTITY:    r['user_identity']  = subele.text
                elif subele.tag == sr.GROUP:            r['group']          = subele.text
                elif subele.tag == sr.GROUP_ATTRIBUTE:
                    group_attr = subele.text
                    attr_type = subele.get(sr.ATTRIBUTE_TYPE)
                    r.setdefault('group_attribute', []).append( (attr_type, group_attr) )

        elif element.tag == sr.START_TIME:              r['start_time']     = parseISODateTime(element.text)
        elif element.tag == sr.END_TIME:                r['end_time']       = parseISODateTime(element.text)
        elif element.tag == sr.RESOURCE_CAPACITY_USED:  r['resource_capacity_used'] = parseInt(element.text)
        elif element.tag == sr.LOGICAL_CAPACITY_USED:   r['logical_capacity_used']  = parseInt(element.text)

        else:
            log.msg("Unhandled storage record element: %s" % element.tag, system='sgas.StorageRecord')

    return r


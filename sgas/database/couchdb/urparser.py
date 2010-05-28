"""
Usage Record parser to convert Usage Records in XML format into CouchDB
documents.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import time
import hashlib

from twisted.python import log

from sgas.usagerecord import ursplitter, urparser



BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"



def base10to62(number):

    BASE = 62

    if number == 0:
        return BASE62[number]

    new_number = ''
    current = number

    while current !=0 :
        remainder = current % BASE
        new_number += BASE62[remainder]
        current = current / BASE

    return ''.join( reversed(new_number) )



# new-style hash, produces smaller/faster b-trees in couchdb
def create12byteb62hash(record_id):
    sha_160_hex = hashlib.sha1(record_id).hexdigest()
    sha_160 = int(sha_160_hex, 16)
    b62_12byte_max_length = 62**12
    b62_hash = base10to62(sha_160 % b62_12byte_max_length)
    return b62_hash


# old-style hash, no longer used
def createSHA224Hash(record_id):
    return hashlib.sha224(record_id).hexdigest()


def createID(record_id):
    hash_id = create12byteb62hash(record_id)
    return hash_id



def usageRecordsToCouchDBDocuments(ur_data, insert_identity=None, insert_hostname=None):

    couchdb_docs = []

    insert_time = time.gmtime()

#    ur_splitter = genericurparser.UsageRecordParser()

    for ur_element in ursplitter.splitURDocument(ur_data):

        cdoc = urparser.xmlToDict(ur_element,
                                  insert_identity=insert_identity,
                                  insert_hostname=insert_hostname)
        # set document type
        cdoc['type'] = 'usagerecord'
        # version control for the convertion in sgas
        # prior to 3.0.0 rc3 there was no such thing, but these are supposed to
        # be version 1
        #
        # convert-version = 2 -- sgas 3.0 rc3
        # The datetime element are properly converted to UTC time and a format
        # which can be parsed
        # by Date.parse in javascript.
        # Usually rc2 and prior versions of datetime can be converted by
        # s/[TZ-]// on the string.
        # However rc2 could really be any form of iso datetime (which was
        # the problem)
        #
        # conver-version = 4 -- sgas 3.1.0
        # use shorted _id field to save b-tree space in couchdb (faster views)
        #
        # the convert-version should be bumped when any changes to the json UR
        # format are made.
        cdoc['convert_version'] = 4
        cdoc['_id'] = createID(cdoc['record_id'])

        couchdb_docs.append(cdoc)

    return couchdb_docs


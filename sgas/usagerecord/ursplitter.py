"""
Usage Record splitter to split an UR XML document into one or more sub elements
each containing a usage record.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""


from xml.etree import cElementTree as ET

from sgas.usagerecord import urelements as ur



class ParseError(Exception):
    """
    Raised when an error occurs during the parsing for splitting up the ur
    elements.
    """


def splitURDocument(self, ur_data):

    usage_records = []

    try:
        tree  = ET.fromstring(ur_data)
    except Exception, e:
        raise ParseError("Error parsing ur document (%s)" % str(e))

    if tree.tag == ur.USAGE_RECORDS:
        for ur_element in tree:
            if not ur_element.tag == ur.JOB_USAGE_RECORD:
                raise ParseError("Subelement in UsageRecords doc not a JobUsageRecord")
            usage_records.append(ur)

    elif tree.tag == ur.JOB_USAGE_RECORD:
        usage_records.append(tree)

    else:
        raise ParseError("Top element is not UsageRecords or JobUsageRecord")

    return usage_records


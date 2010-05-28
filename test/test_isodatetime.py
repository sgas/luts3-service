#
# Database unit tests
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import datetime

from twisted.trial import unittest

from sgas.ext import isodate
from sgas.usagerecord import urparser



ISO_DATETIME_STRINGS = [
 '2009-11-12T20:31:27Z',
 '2009-W46-4T20:31:27Z'
]

DATETIME = datetime.datetime(2009, 11, 12, 20, 31, 27, tzinfo=isodate.UTC)
JSON_DATETIME = "2009 11 12 20:31:27"


ISO_DURATION_STRINGS = [
 'PT86401S',
 'P0Y0M1DT0H0M1S'
]

DURATION_SECONDS = 86401



class ISODateTimeTest(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testParsing(self):

        for dts in ISO_DATETIME_STRINGS:
            dt = isodate.parse_datetime(dts)
            self.failUnlessEqual(dt, DATETIME)


    def testConversion(self):

        for dts in ISO_DATETIME_STRINGS:
            json_dt = urparser.parseISODateTime(dts)
            self.failUnlessEqual(json_dt, JSON_DATETIME)


class ISODurationTest(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testParsing(self):

        for tds in ISO_DURATION_STRINGS:
            td = isodate.parse_duration(tds)
            seconds = (td.days * 3600*24) + td.seconds # we ignore microseconds
            self.failUnlessEqual(seconds, DURATION_SECONDS)


    def testConversion(self):

        for tds in ISO_DURATION_STRINGS:
            ss = urparser.parseISODuration(tds)
            self.failUnlessEqual(ss, DURATION_SECONDS)



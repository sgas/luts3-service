"""
Tests for the query parser.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from twisted.trial import unittest

from sgas.database import queryparser



class QueryParserTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testAttributeRegEx(self):

        r1 = queryparser.ATTRIBUTE_REGEX.match('user_identity')
        if not r1:
            self.fail('No match for valid input')
        self.failUnlessEqual(r1.groups(), ('user_identity', None, None))

        r2 = queryparser.ATTRIBUTE_REGEX.match('sum:machine_name')
        if not r2:
            self.fail('No match for valid input')
        self.failUnlessEqual(r2.groups(), ('sum', ':machine_name', 'machine_name'))


        rf1 = queryparser.ATTRIBUTE_REGEX.match('sum:machine_name:')
        if rf1:
            self.fail('Match for invalid input')

        rf2 = queryparser.ATTRIBUTE_REGEX.match(':machine_name')
        if rf2:
            self.fail('Match for invalid input')

        rf2 = queryparser.ATTRIBUTE_REGEX.match(':')
        if rf2:
            self.fail('Match for invalid input')


    def testFilterRegEx(self):

        r1 = queryparser.FILTER_REGEX.match("thing='stuff'")
        if not r1:
            self.fail('No match for valid input')
        self.failUnlessEqual(r1.groups(), ('thing', '=', "'stuff'"))

        r2 = queryparser.FILTER_REGEX.match('thing>7')
        if not r2:
            self.fail('No match for valid input')
        self.failUnlessEqual(r2.groups(), ('thing', '>', '7'))

        r3 = queryparser.FILTER_REGEX.match('thing^start')
        if not r3:
            self.fail('No match for valid input')
        self.failUnlessEqual(r3.groups(), ('thing', '^', 'start'))

        r4 = queryparser.FILTER_REGEX.match('thing$end')
        if not r4:
            self.fail('No match for valid input')
        self.failUnlessEqual(r4.groups(), ('thing', '$', 'end'))

        r5 = queryparser.FILTER_REGEX.match('thing >=0')
        if not r5:
            self.fail('No match for valid input')
        self.failUnlessEqual(r5.groups(), ('thing', '>=', '0'))

        r6 = queryparser.FILTER_REGEX.match('thing = 0')
        if not r6:
            self.fail('No match for valid input')
        self.failUnlessEqual(r6.groups(), ('thing', '=', '0'))

        r7 = queryparser.FILTER_REGEX.match('n_jobs > 200')
        if not r7:
            self.fail('No match for valid input')
        self.failUnlessEqual(r7.groups(), ('n_jobs', '>', '200'))

        rf1 = queryparser.FILTER_REGEX.match('thing')
        if rf1:
            self.fail('Match for invalid input')

        rf2 = queryparser.FILTER_REGEX.match('thing=')
        if rf2:
            self.fail('Match for invalid input')

        rf3 = queryparser.FILTER_REGEX.match('thing 3')
        if rf3:
            self.fail('Match for invalid input')



    def testParser(self):

        qp1 = queryparser.QueryParser('user_identity')
        self.failUnlessEqual(len(qp1.selects), 1)
        self.failUnlessEqual(len(qp1.filters), 0)
        self.failUnlessEqual(len(qp1.groups), 0)
        self.failUnlessEqual(len(qp1.orders), 0)
        self.failUnlessEqual(qp1.selects[0].attribute, 'user_identity')
        self.failUnlessEqual(qp1.selects[0].aggregator, None)

        qp2 = queryparser.QueryParser('user_identity', 'n_jobs > 200' )
        self.failUnlessEqual(len(qp2.selects), 1)
        self.failUnlessEqual(len(qp2.filters), 1)
        self.failUnlessEqual(len(qp2.groups), 0)
        self.failUnlessEqual(len(qp2.orders), 0)
        self.failUnlessEqual(qp2.selects[0].attribute, 'user_identity')
        self.failUnlessEqual(qp2.selects[0].aggregator, None)
        self.failUnlessEqual(qp2.filters[0].attribute, 'n_jobs')
        self.failUnlessEqual(qp2.filters[0].operator, '>')
        self.failUnlessEqual(qp2.filters[0].value, '200')

        qp3 = queryparser.QueryParser('user_identity, sum:n_jobs', 'n_jobs > 200', orders='n_jobs')
        self.failUnlessEqual(len(qp3.selects), 2)
        self.failUnlessEqual(len(qp3.filters), 1)
        self.failUnlessEqual(len(qp3.groups), 0)
        self.failUnlessEqual(len(qp3.orders), 1)
        self.failUnlessEqual(qp3.selects[0].attribute, 'user_identity')
        self.failUnlessEqual(qp3.selects[0].aggregator, None)
        self.failUnlessEqual(qp3.selects[1].attribute, 'n_jobs')
        self.failUnlessEqual(qp3.selects[1].aggregator, 'sum')
        self.failUnlessEqual(qp3.filters[0].attribute, 'n_jobs')
        self.failUnlessEqual(qp3.filters[0].operator, '>')
        self.failUnlessEqual(qp3.filters[0].value, '200')
        self.failUnlessEqual(qp3.orders[0].attribute, 'n_jobs')

        qp4 = queryparser.QueryParser('user_identity, sum:n_jobs', groups='user_identity')
        self.failUnlessEqual(len(qp4.selects), 2)
        self.failUnlessEqual(len(qp4.filters), 0)
        self.failUnlessEqual(len(qp4.groups), 1)
        self.failUnlessEqual(len(qp4.orders), 0)
        self.failUnlessEqual(qp4.selects[0].attribute, 'user_identity')
        self.failUnlessEqual(qp4.selects[0].aggregator, None)
        self.failUnlessEqual(qp4.selects[1].attribute, 'n_jobs')
        self.failUnlessEqual(qp4.selects[1].aggregator, 'sum')
        self.failUnlessEqual(qp4.groups[0].attribute, 'user_identity')


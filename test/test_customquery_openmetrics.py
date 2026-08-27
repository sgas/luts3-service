#
# Custom query engine openmetrics output unit tests
#
# Author: Erik Edelmann <erik.edelmann@csc.fi>
# Copyright: NeIC (2026)

from twisted.trial import unittest

from sgas.server import config
from sgas.customqueryengine import querydefinition, openmetrics


def makeQuery(query_name='testquery', **extra_config):
    query_config = {
        'querygroup' : 'admin',
        'query'      : 'SELECT machine_name, n_jobs FROM foo',
    }
    query_config.update(extra_config)
    return querydefinition.createQueryDefinition(query_name, query_config)


class QueryDefinitionMetricConfigTest(unittest.TestCase):

    def testDefaults(self):
        q = makeQuery()
        self.failUnlessEqual(q.metric_name, 'testquery')
        self.failUnlessEqual(q.metric_type, 'gauge')
        self.failUnlessIdentical(q.metric_help, None)
        self.failUnlessIdentical(q.metric_value, None)
        self.failUnlessIdentical(q.metric_labels, None)

    def testExplicitMetricName(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs')
        self.failUnlessEqual(q.metric_name, 'sgas_jobs')

    def testCounterGetsTotalSuffixAppended(self):
        q = makeQuery(metric_value='n_jobs', metric_type='counter', metric_name='sgas_jobs')
        self.failUnlessEqual(q.metric_name, 'sgas_jobs_total')

    def testCounterAlreadyEndingInTotalIsLeftAlone(self):
        q = makeQuery(metric_value='n_jobs', metric_type='counter', metric_name='sgas_jobs_total')
        self.failUnlessEqual(q.metric_name, 'sgas_jobs_total')

    def testInvalidMetricTypeRejected(self):
        self.failUnlessRaises(config.ConfigurationError, makeQuery,
                               metric_value='n_jobs', metric_type='histogram')

    def testMetricLabelsWithoutMetricValueRejected(self):
        self.failUnlessRaises(config.ConfigurationError, makeQuery, metric_labels='machine_name')

    def testMetricLabelsParsedAsList(self):
        q = makeQuery(metric_value='n_jobs', metric_labels='machine_name, vo')
        self.failUnlessEqual(q.metric_labels, ['machine_name', 'vo'])


class OpenMetricsRenderTest(unittest.TestCase):

    def testRenderNotConfigured(self):
        q = makeQuery()
        self.failUnlessRaises(openmetrics.OpenMetricsError, openmetrics.render, q, [])

    def testRenderGaugeWithLabels(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs', metric_help='Number of jobs')
        rows = [
            {'machine_name' : 'host1.example.org', 'n_jobs' : 3},
            {'machine_name' : 'host2.example.org', 'n_jobs' : 7},
        ]
        payload = openmetrics.render(q, rows).decode('utf-8')
        lines = payload.splitlines()

        self.failUnlessEqual(lines[0], '# HELP sgas_jobs Number of jobs')
        self.failUnlessEqual(lines[1], '# TYPE sgas_jobs gauge')
        self.failUnlessIn('sgas_jobs{machine_name="host1.example.org"} 3', lines)
        self.failUnlessIn('sgas_jobs{machine_name="host2.example.org"} 7', lines)
        self.failUnlessEqual(lines[-1], '# EOF')

    def testRenderCounterAppendsTotalSuffix(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs', metric_type='counter')
        payload = openmetrics.render(q, [{'machine_name' : 'host1', 'n_jobs' : 3}]).decode('utf-8')

        self.failUnlessIn('# TYPE sgas_jobs_total counter', payload)
        self.failUnlessIn('sgas_jobs_total{machine_name="host1"} 3', payload)

    def testRenderExplicitLabelSubset(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs', metric_labels='machine_name')
        rows = [{'machine_name' : 'host1', 'vo' : 'atlas', 'n_jobs' : 3}]
        payload = openmetrics.render(q, rows).decode('utf-8')

        self.failUnlessIn('sgas_jobs{machine_name="host1"} 3', payload)
        self.failIfIn('vo=', payload)

    def testRenderNoLabels(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs', metric_labels='')
        rows = [{'machine_name' : 'host1', 'n_jobs' : 3}]
        payload = openmetrics.render(q, rows).decode('utf-8')

        # metric_labels='' parses to [''], i.e. no real label columns
        self.failUnlessIn('sgas_jobs 3', payload)

    def testRenderNullValueRejected(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs')
        self.failUnlessRaises(openmetrics.OpenMetricsError, openmetrics.render, q,
                               [{'machine_name' : 'host1', 'n_jobs' : None}])

    def testRenderNonNumericValueRejected(self):
        q = makeQuery(metric_value='machine_name', metric_name='sgas_jobs')
        self.failUnlessRaises(openmetrics.OpenMetricsError, openmetrics.render, q,
                               [{'machine_name' : 'host1', 'n_jobs' : 3}])

    def testRenderNumericStringCoerced(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs', metric_labels='')
        payload = openmetrics.render(q, [{'n_jobs' : '3.5'}]).decode('utf-8')
        self.failUnlessIn('sgas_jobs 3.5', payload)

    def testLabelValueEscaping(self):
        q = makeQuery(metric_value='n_jobs', metric_name='sgas_jobs', metric_labels='machine_name')
        rows = [{'machine_name' : 'weird "host"\\name', 'n_jobs' : 1}]
        payload = openmetrics.render(q, rows).decode('utf-8')

        self.failUnlessIn('machine_name="weird \\"host\\"\\\\name"', payload)

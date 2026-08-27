"""
View definition module. Parser and representation of a view.
Part of the SGAS view engine.

Author: Magnus Jonsson <magnus@hpc2n.umu.se>
Copyright: Nordic Data Grid Facility (2012)
"""


from twisted.python import log
from sgas.server import config

import re

# query options
QUERY_PREFIX      = 'query:'
QUERY_GROUP       = 'querygroup'
QUERY_QUERY       = 'query'
QUERY_PARAMS      = 'params'
QUERY_AUTHZ       = 'authz_params'

# openmetrics output options
QUERY_METRIC_NAME   = 'metric_name'
QUERY_METRIC_NAME_COLUMN   = 'metric_name_column'
QUERY_METRIC_TYPE   = 'metric_type'
QUERY_METRIC_HELP   = 'metric_help'
QUERY_METRIC_VALUE  = 'metric_value'
QUERY_METRIC_LABELS = 'metric_labels'

METRIC_TYPE_GAUGE   = 'gauge'
METRIC_TYPE_COUNTER = 'counter'
VALID_METRIC_TYPES  = (METRIC_TYPE_GAUGE, METRIC_TYPE_COUNTER)

COUNTER_SUFFIX = '_total'


class QueryParseError(Exception):
    """
    Thrown if invalid arguments or data is given to the query parser.
    """


class QueryDefinition:

    def __init__(self, query_name, query_group, query, params, authz_params,
                 metric_name=None, metric_name_column=None, metric_type=METRIC_TYPE_GAUGE, metric_help=None,
                 metric_value=None, metric_labels=None):
        self.query_name  = query_name
        self.query_group = query_group
        self.query       = query
        self.params      = params
        self.authz_params = authz_params

        # openmetrics output configuration; metric_value is None unless
        # the query has been configured for openmetrics output
        self.metric_name   = metric_name or query_name
        self.metric_name_column   = metric_name_column
        self.metric_type   = metric_type
        self.metric_help   = metric_help
        self.metric_value  = metric_value
        self.metric_labels = metric_labels

        if self.metric_type == METRIC_TYPE_COUNTER and not self.metric_name.endswith(COUNTER_SUFFIX):
            self.metric_name += COUNTER_SUFFIX

    def parseURLArguments(self, request_args):
        # ensure all arguments are understood / allowed
        for query_field in request_args:
            if query_field.decode('utf-8') not in self.params:
                raise QueryParseError('Query field "%s" not understood/allowed.' % query_field)

        result = {}
        # ensure all arguments are understood / allowed
        for query_field in self.params:
            if query_field:
                if query_field.encode('utf-8') not in request_args:
                    raise QueryParseError('Query field "%s" missing.' % query_field)
                result[query_field] = request_args[query_field.encode('utf-8')][0].decode('utf-8')
            
        return result


def buildQueryList(cfg):
    
    queries = []
    
    for block in cfg.sections():
        if block.startswith(QUERY_PREFIX):
            query_name = block.split(':',1)[-1]
            query_args = dict(cfg.items(block))
            query = createQueryDefinition(query_name, query_args)
            queries.append(query)

    return queries



def createQueryDefinition(query_name, query_config):

    query_groups = []
    query        = None
    params       = []
    authz_params = []
    metric_name   = None
    metric_name_column   = None
    metric_type   = METRIC_TYPE_GAUGE
    metric_help   = None
    metric_value  = None
    metric_labels = None

    for key, value in query_config.items():
        if key == QUERY_GROUP:
            query_groups = [ group.strip() for group in value.split(',') ]

        elif key == QUERY_QUERY:
            query = value

        elif key == QUERY_PARAMS:
            params = [ param.strip() for param in value.split(',') ]

        elif key == QUERY_AUTHZ:
            authz_params = [ param.strip() for param in value.split(',') ]

        elif key == QUERY_METRIC_NAME:
            metric_name = value.strip()

        elif key == QUERY_METRIC_NAME_COLUMN:
            metric_name_column = value.strip()

        elif key == QUERY_METRIC_TYPE:
            metric_type = value.strip()

        elif key == QUERY_METRIC_HELP:
            metric_help = value.strip()

        elif key == QUERY_METRIC_VALUE:
            metric_value = value.strip()

        elif key == QUERY_METRIC_LABELS:
            metric_labels = [ label.strip() for label in value.split(',') if label.strip() ]

        else:
            log.msg("Unknown query definition key: %s" % key, system='customQueryEngine.ViewDefinition')
            raise config.ConfigurationError("Unknown key: %s" % key)

    if query_name in (None, ''):
        raise config.ConfigurationError('Missing or empty query name for query definition')
    if query in (None, ''):
        raise config.ConfigurationError('Missing or empty query for view definition')
    if metric_type not in VALID_METRIC_TYPES:
        raise config.ConfigurationError('Invalid metric_type "%s" for query "%s", must be one of: %s' % \
                                         (metric_type, query_name, ', '.join(VALID_METRIC_TYPES)))
    if metric_labels is not None and metric_value is None:
        raise config.ConfigurationError('metric_labels given without metric_value for query "%s"' % query_name)

    # ConfigParser also uses %(xxx) for reading variables from the config file.
    # "Workaround" using diffrent tag.
    query = re.sub(r'<<([^>]+)>>',r'%(\1)s',query)

    return QueryDefinition(query_name, query_groups, query, params, authz_params,
                            metric_name, metric_name_column, metric_type, metric_help, metric_value, metric_labels)


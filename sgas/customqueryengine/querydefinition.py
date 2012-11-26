"""
View definition module. Parser and representation of a view.
Part of the SGAS view engine.

Author: Magnus Jonsson <magnus@hpc2n.umu.se>
Copyright: Nordic Data Grid Facility (2012)
"""


from twisted.python import log
from sgas.server import config

import re

class QueryParseError(Exception):
    """
    Thrown if invalid arguments or data is given to the query parser.
    """


class QueryDefinition:

    def __init__(self, query_name, query_group, query, params):        
        self.query_name  = query_name        
        self.query_group = query_group
        self.query       = query
        self.params      = params
                
    def parseURLArguments(self, request_args):
        # ensure all arguments are understood / allowed
        for query_field in request_args:
            if query_field not in self.params:
                raise QueryParseError('Query field "%s" not understood/allowed.' % query_field)

        result = {}
        # ensure all arguments are understood / allowed
        for query_field in self.params:
            if query_field:
                if query_field not in request_args:
                    raise QueryParseError('Query field "%s" missing.' % query_field)
                result[query_field] = request_args[query_field][0]
            
        return result


def buildQueryList(cfg):
    
    queries = []
    
    for block in cfg.sections():
        if block.startswith(config.QUERY_PREFIX):
            query_name = block.split(':',1)[-1]
            query_args = dict(cfg.items(block))
            query = createQueryDefinition(query_name, query_args)
            queries.append(query)

    return queries



def createQueryDefinition(query_name, query_config):
    
    query_groups = []    
    query        = None
    params       = []

    for key, value in query_config.items():
        if key == config.QUERY_GROUP:
            query_groups = [ group.strip() for group in value.split(',') ]

        elif key == config.QUERY_QUERY:
            query = value

        elif key == config.QUERY_PARAMS:
            params = [ param.strip() for param in value.split(',') ]

        else:
            log.msg("Unknown query definition key: %s" % key, system='customQueryEngine.ViewDefinition')
            raise config.ConfigurationError("Unknown key: %s" % key)            

    if query_name in (None, ''):
        raise config.ConfigurationError('Missing or empty query name for query definition')
    if query in (None, ''):
        raise config.ConfigurationError('Missing or empty query for view definition')
    
    # ConfigParser also uses %(xxx) for reading variables from the config file.
    # "Workaround" using diffrent tag.
    query = re.sub(r'<<(\S+)>>',r'%(\1)s',query)

    return QueryDefinition(query_name, query_groups, query, params)


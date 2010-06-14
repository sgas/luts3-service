"""
Query engine for the SGAS PostgreSQL backend. Essentially a translator from
the SGAS query language to PostgreSQL statements.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

# FIXME all db input should be properly quoted


AGGREGATION_TABLE = 'uraggregated'



def buildQuery(query):

    def buildSelector(selector, params):
        if selector.aggregator is None:
            return selector.attribute
        else:
            return selector.aggregator + '(' + selector.attribute + ')'

    def buildFilter(filter, params):
        if filter.operator in ('=','!=','<','>','<=','>='):
            return "%s %s '%s'" % (filter.attribute, filter.operator, filter.value)
        elif filter.operator == '^':
            return "%s LIKE '%s%'" % (filter.attribute, filter.value)
        elif filter.operator == '$':
            return "%s LIKE '%%%s'" % (filter.attribute, filter.value)
        else:
            raise NotImplementedError('Unsupported filter operator: %s' % filter.operator)

    params = {}

    selects = ','.join( [ buildSelector(s, params) for s in query.selects ] )
    filters = ','.join( [ buildFilter(f, params)   for f in query.filters ] )
    groups  = ','.join( [ g.attribute              for g in query.groups  ] )
    orders  = ','.join( [ o.attribute              for o in query.orders  ] )

    query_stm = 'SELECT %s FROM %s' % (selects, AGGREGATION_TABLE)
    if filters:
        query_stm += ' WHERE %s' % filters
    if groups:
        query_stm += ' GROUP BY %s' % groups
    if orders:
        query_stm += ' ORDER BY %s' % orders

    return query_stm



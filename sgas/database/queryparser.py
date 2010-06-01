"""
Parser for the generic SGAS query language.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

import re

from sgas.database import query



# attribute | aggregator:attribute
ATTRIBUTE_REGEX = re.compile('^\s*(\w+)(:(\w+))?\s*$')

# key compare-operator value
FILTER_REGEX = re.compile("^([\w_]+)\s*(=|!=|<|>|<=|>=|\^|\$)\s*('?[\.\w]+'?)$")



class QueryParser:

    def __init__(self, selects, filters=None, groups=None, orders=None):

        self.selects = self._parseSelects(selects)
        self.filters = self._parseFilters(filters)
        self.groups = self._parseGroups(groups)
        self.orders = self._parseOrders(orders)


    def _parseSelects(self, selects):

        if type(selects) is str:
            selects = selects.split(',')

        parsed_selects = [ ATTRIBUTE_REGEX.match(select) for select in selects ]
        def buildQuerySelect(ps):
            if ps.group(3) is None:
                return query.QuerySelect(ps.group(1), ps.group(3))
            else:
                return query.QuerySelect(ps.group(3), ps.group(1))

        select_attributes = [ buildQuerySelect(ps) for ps in parsed_selects ]
        return select_attributes


    def _parseFilters(self, filters):

        if filters is None:
            return []

        if type(filters) is str:
            filters = filters.split(',')

        parsed_filters = [ FILTER_REGEX.match(filter) for filter in filters ]
        filter_attributes = [ query.QueryFilter(pf.group(1), pf.group(2), pf.group(3)) for pf in parsed_filters ]
        return filter_attributes


    def _parseGroups(self, groups):

        if groups is None:
            return []

        if type(groups) is str:
            groups = groups.split(',')

        group_attributes = [ query.QueryGroup(group) for group in groups ]
        return group_attributes


    def _parseOrders(self, orders):

        if orders is None:
            return []

        if type(orders) is str:
            orders = orders.split(',')

        order_attributes = [ query.QueryOrder(order) for order in orders ]
        return order_attributes


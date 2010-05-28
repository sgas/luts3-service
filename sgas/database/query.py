"""
Common query functionality for the generic SGAS query language.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""



class QuerySelect:

    def __init__(self, attribute, aggregator=None):
        self.attribute = attribute
        self.aggregator = aggregator



class QueryFilter:

    def __init__(self, attribute, operator, value):
        self.attribute = attribute
        self.operator = operator
        self.value = value



class QueryGroup:

    def __init__(self, attribute):
        self.attribute = attribute



class QueryOrder:

    def __init__(self, attribute):
        self.attribute = attribute


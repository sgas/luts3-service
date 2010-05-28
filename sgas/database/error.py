"""
Generic errors for the SGAS database module.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""


class SGASDatabaseError(Exception):
    """
    Top-level exception for all SGAS database errors.
    """


class DatabaseUnavailableError(SGASDatabaseError):
    """
    Error raised when the database is unavailable.
    """


class InvalidUsageDataError(SGASDatabaseError):
    """
    Error raised when usage record data is invalid.
    """


class InvalidQueryError(SGASDatabaseError):
    """
    Error raised when a query is invalid.
    """


class DatabaseQueryError(SGASDatabaseError):
    """
    Error raised when a query threw an error.
    """


"""
Interface decleration for SGAS database backend.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from zope.interface import Interface



class ISGASDatabase(Interface):
    """
    The task of the database interface is to encapsulate the storage, querying
    and optional transformation of usage records in SGAS.

    This is done by providing a small set of methods and functionality which
    should be generic enough to be implementable on a variety of databases (not
    just relational), while also being usefull enough that direct database
    access can be avoided.

    The database should as minumum support all usage record attributes, but
    extended attributes which are used should be supported if at all possible.
    """

    def insert(usagerecord_data, insert_identity=None, insert_hostname=None):
        """
        Inserts one or more usage records into the database. If one or more of
        the inserted usage records already exist in the database duplicate
        detection should be made and the insert should be acknowledged as if
        the usage record had been inserted.

        @param usagerecord_data: A string contianing an XML document adhering
                                 to the OGF Usage Record standard (OGF-98).
                                 Such an XML document can contain one or more
                                 usage records. Additional attributes may be
                                 present in the documents, but are not
                                 guarantied to be present in the underlying
                                 database after insertion.

        @param insert_identity:  A string containining the authenticated
                                 identity of the entity inserting the
                                 usage records. None is allowed.

        @param insert_hostname:  The hostname of the originating request for
                                 inserting the usage records. None is allowed.
        """



    def query(query, query_args=None):
        """
        Queries the database, returning rows of information.

        The following entry/column names should be supported for querying:
        * execution_date : Day the job(s) was executed.
        * insert_date    : Day the usage record was inserted into the database.
        * machine_name   : Host FQDN of the machine executing the job.
        * user_identity  : Name of the user who submitted the job.
        * vo_issuer      : Identity of the VO issuer
        * vo_name        : Name of the primary VO for the user identity.
        * vo_group       : Name of the VO group of the user identity.
        * vo_role        : Name of the VO role of the user identity.
        * n_jobs         : Number of jobs the user executed.
        * cpuhours       : Number of CPU hours used by the jobs.
        * wallhours      : Number of Wall hours used by the jobs.
        * generate_time  : Timestamp for when the entry was generated.

        @param query     : A string describing the query.

        @param query_args : An optional dictoary of paramteres that should be
                            interpolated into the query.
        """


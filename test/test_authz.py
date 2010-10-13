#
# Authorization tests
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2010)

from twisted.trial import unittest

from sgas.server import authz

# includes all basic authz types and some basic combinations
# there will probably be more later
SAMPLE_AUTHZ_DATA = """
"host1"     insert
"host2"     insert:all
"host3"     insert:machine_name=host2
# comment
"user1"     view
"user2"     view:view=viewname
"user3"     view:group=vg1
"user4"     view:group=vg1, view:group=vg2
"user5"     view:group=vg1;vg2
"user6"     view:all

"bot1"      query:all
"bot2"      query:machine_name=host1
"bot3"      query:user_identity=user1
"bot4"      query:user_identity=user1, query:user_identity=user3
"bot5"      query:machine_name=host2+user_identity=user2
"bot6"      query:machine_name=host2+user_identity=user2;user4
"bot7"      query:vo_name=vo1
"""


class AuthzTest(unittest.TestCase):


    def setUp(self):
        self.authz = authz.Authorizer()
        self.authz.parseAuthzData(SAMPLE_AUTHZ_DATA)


    def tearDown(self):
        pass


    def testInsertAuthz(self):
        # "host1"     insert
        # "host2"     insert:all
        # "host3"     insert:machine_name=host2

        self.failUnlessFalse( self.authz.isAllowed('host_unknown', authz.INSERT))

        self.failUnlessTrue(  self.authz.isAllowed('host1', authz.INSERT))
        self.failUnlessFalse( self.authz.isAllowed('host1', authz.INSERT, context={'machine_name': 'host2'}) )

        self.failUnlessTrue(  self.authz.isAllowed('host2', authz.INSERT) )
        self.failUnlessTrue(  self.authz.isAllowed('host2', authz.INSERT, context={'machine_name': 'host1'}) )
        self.failUnlessTrue(  self.authz.isAllowed('host2', authz.INSERT, context={'machine_name': 'host3'}) )
        # can a host insert a usage record with a certain user - yes it can
        # doesn't make a that much of sense, but it can
        self.failUnlessTrue(  self.authz.isAllowed('host2', authz.INSERT, context={'user_identity': 'user1'}) )

        self.failUnlessTrue(  self.authz.isAllowed('host3', authz.INSERT) )
        self.failUnlessTrue(  self.authz.isAllowed('host3', authz.INSERT, context={'machine_name': 'host2'}) )
        self.failUnlessFalse( self.authz.isAllowed('host3', authz.INSERT, context={'machine_name': 'host1'}) )

        self.failUnlessFalse( self.authz.isAllowed('user1', authz.INSERT) )
        self.failUnlessFalse( self.authz.isAllowed('bot1',  authz.INSERT) )


    def testViewAuthz(self):
        # "user1"     view
        # "user2"     view:view=viewname
        # "user3"     view:group=vg1
        # "user4"     view:group=vg1, view:group=vg2
        # "user5"     view:group=vg1;vg2
        # "user6"     view:all

        self.failUnlessTrue  ( self.authz.isAllowed('user1', authz.VIEW, context={'view': 'viewname' }) )
        self.failUnlessTrue  ( self.authz.isAllowed('user1', authz.VIEW, context={'group': 'vg1' }) )

        self.failUnlessTrue  ( self.authz.isAllowed('user2', authz.VIEW, context={'view': 'viewname' }) )
        self.failUnlessFalse ( self.authz.isAllowed('user2', authz.VIEW, context={'view': 'otherview'}) )

        self.failUnlessTrue  ( self.authz.isAllowed('user3', authz.VIEW, context={'group': 'vg1'}) )
        self.failUnlessFalse ( self.authz.isAllowed('user3', authz.VIEW, context={'group': 'vg2'}) )
        self.failUnlessFalse ( self.authz.isAllowed('user3', authz.VIEW, context={'view': 'viewname'}) )
        self.failUnlessFalse ( self.authz.isAllowed('user3', authz.VIEW, context={'view': 'otherview'}) )
        self.failUnlessTrue  ( self.authz.isAllowed('user3', authz.VIEW, context={'view': 'otherview', 'group': 'vg1'} ) )

        self.failUnlessFalse ( self.authz.isAllowed('user4', authz.VIEW, context={ 'group': 'vg3'} ) )
        self.failUnlessFalse ( self.authz.isAllowed('user4', authz.VIEW, context={ 'view': 'viewname'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user4', authz.VIEW, context={ 'group': 'vg1'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user4', authz.VIEW, context={ 'view': 'otherview', 'group': 'vg2'} ) )

        self.failUnlessFalse ( self.authz.isAllowed('user5', authz.VIEW, context={ 'group': 'vg3'} ) )
        self.failUnlessFalse ( self.authz.isAllowed('user5', authz.VIEW, context={ 'view': 'viewname'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user5', authz.VIEW, context={ 'group': 'vg1'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user5', authz.VIEW, context={ 'view': 'otherview', 'group': 'vg2'} ) )

        self.failUnlessTrue  ( self.authz.isAllowed('user6', authz.VIEW, context={ 'group': 'vg3'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user6', authz.VIEW, context={ 'view': 'viewname'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user6', authz.VIEW, context={ 'group': 'vg1'} ) )
        self.failUnlessTrue  ( self.authz.isAllowed('user5', authz.VIEW, context={ 'view': 'otherview', 'group': 'vg2'} ) )


    def testQueryAuthz(self):
        # "bot1"      query:all
        # "bot2"      query:machine_name=host1
        # "bot3"      query:user_identity=user1
        # "bot4"      query:user_identity=user1, query:user_identity=user3
        # "bot5"      query:machine_name=host2+user_identity=user2
        # "bot6"      query:machine_name=host2+user_identity=user2;user4
        # "bot7"      query:vo_name=vo1

        self.failUnlessTrue  ( self.authz.isAllowed('bot1', authz.QUERY, context={ 'machine_name': 'host1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot1', authz.QUERY, context={ 'user_identity': 'user1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot1', authz.QUERY, context={ 'machine_name': 'host1', 'user_identity': 'user2' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot1', authz.QUERY, context={ 'vo_name': 'vo1' } ) )

        self.failUnlessTrue  ( self.authz.isAllowed('bot2', authz.QUERY, context={ 'machine_name': 'host1' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot2', authz.QUERY, context={ 'machine_name': 'host2' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot2', authz.QUERY, context={ 'user_identity': 'user1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot2', authz.QUERY, context={ 'machine_name': 'host1', 'user_identity': 'user1' } ) )

        self.failUnlessFalse ( self.authz.isAllowed('bot3', authz.QUERY, context={ 'machine_name': 'host1' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot3', authz.QUERY, context={ 'machine_name': 'host2' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot3', authz.QUERY, context={ 'user_identity': 'user1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot3', authz.QUERY, context={ 'machine_name': 'host1', 'user_identity': 'user1' } ) )

        self.failUnlessFalse ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'machine_name': 'host1' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'machine_name': 'host2' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'user_identity': 'user1' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'user_identity': 'user2' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'user_identity': 'user3' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'machine_name': 'host1', 'user_identity': 'user1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user3' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot4', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user2' } ) )

        self.failUnlessFalse ( self.authz.isAllowed('bot5', authz.QUERY, context={ 'machine_name': 'host2' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot5', authz.QUERY, context={ 'user_identity': 'user2' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot5', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user2' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot5', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user1' } ) )

        self.failUnlessFalse ( self.authz.isAllowed('bot6', authz.QUERY, context={ 'machine_name': 'host1' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot6', authz.QUERY, context={ 'machine_name': 'host2' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot6', authz.QUERY, context={ 'user_identity': 'user2' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot6', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user2' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot6', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user3' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot6', authz.QUERY, context={ 'machine_name': 'host2', 'user_identity': 'user4' } ) )

        self.failUnlessFalse ( self.authz.isAllowed('bot7', authz.QUERY, context={ 'machine_name': 'host2' } ) )
        self.failUnlessFalse ( self.authz.isAllowed('bot7', authz.QUERY, context={ 'user_identity': 'user1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot7', authz.QUERY, context={ 'vo_name': 'vo1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot7', authz.QUERY, context={ 'machine_name': 'host2', 'vo_name': 'vo1' } ) )
        self.failUnlessTrue  ( self.authz.isAllowed('bot7', authz.QUERY, context={ 'user_identity': 'user1', 'vo_name': 'vo1' } ) )


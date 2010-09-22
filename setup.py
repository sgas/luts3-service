import os
from distutils.core import setup
from distutils.command.install_data import install_data

from sgas import __version__


class InstallSGASData(install_data):
    # this class is used to filter out data files which should not be overwritten
    # currently this is sgas.conf and sgas.authz

    def finalize_options(self):
        install_data.finalize_options(self)

        sgas_conf = '/etc/sgas.conf'
        if self.root is not None:
            sgas_conf = os.path.join(self.root, sgas_conf[1:])

        if os.path.exists(sgas_conf):
            print "Skipping installation of etc/sgas.conf and etc/sgas.authz (already exists)"
            self.data_files.remove( ('/etc/', ['datafiles/etc/sgas.conf']) )
            self.data_files.remove( ('/etc/', ['datafiles/etc/sgas.authz']) )


cmdclasses = {'install_data': InstallSGASData} 


setup(name='sgas-luts-service',
      version=__version__,
      description='SGAS LUTS Accounting Server',
      author='Henrik Thostrup Jensen',
      author_email='htj@ndgf.org',
      url='http://www.sgas.se/',
      packages=['sgas', 'sgas/database', 'sgas/database/couchdb', 'sgas/database/postgresql',
                'sgas/ext', 'sgas/ext/isodate', 'sgas/ext/python', 'sgas/server', 'sgas/usagerecord', 'sgas/viewengine'],

      cmdclass = cmdclasses,

      data_files=[
          ('share/sgas',               ['datafiles/share/sgas.tac']),
          ('share/sgas/webfiles/css',  ['datafiles/share/webfiles/css/view.frontpage.css',
                                        'datafiles/share/webfiles/css/view.table.css']),
          ('share/sgas/webfiles/js',   ['datafiles/share/webfiles/js/protovis-d3.1.js',
                                        'datafiles/share/webfiles/js/protovis-r3.1.js']),
          ('share/sgas/postgres',      ['datafiles/share/postgresql/sgas-postgres-schema.sql',
                                        'datafiles/share/postgresql/sgas-postgres-view.sql',
                                        'datafiles/share/postgresql/sgas-postgres-functions.sql',
                                        'datafiles/share/postgresql/sgas-postgres-schema-drop.sql',
                                        'datafiles/share/postgresql/sgas-postgres-3.2-3.3-upgrade.sql',
                                        'datafiles/share/postgresql/sgas-postgres-3.3-3.4-upgrade.sql',
                                        'datafiles/share/postgresql/sgas-postgres-aggregation-rebuild.sql',
                                        'datafiles/share/postgresql/sgas-postgres-cluster.sql']),
          ('/etc/init.d',              ['datafiles/etc/sgas']),
          ('/etc/',                    ['datafiles/etc/sgas.conf']),
          ('/etc/',                    ['datafiles/etc/sgas.authz'])
      ]

)


import os
from distutils.core import setup
from distutils.command.install import install
from distutils.command.install_data import install_data

from sgas import __version__


# nasty global for relocation
RELOCATE = None

class InstallSGAS(install):

    def finalize_options(self):
        install.finalize_options(self)

        global RELOCATE ; RELOCATE = self.home



class InstallSGASData(install_data):
    # this class is used for relocating datafiles, and remove existing etc files
    # so we don't overwrite the configuration of existing sites

    def finalize_options(self):
        install_data.finalize_options(self)

        # relocation
        if RELOCATE:
            print 'relocating to %s' % RELOCATE
            for (prefix, files) in reversed(self.data_files):
                if prefix.startswith('/'):
                    new_prefix = os.path.join(RELOCATE, prefix[1:])
                    self.data_files.remove((prefix, files))
                    self.data_files.append((new_prefix, files))

        # check that we don't overwrite /etc files
        for (prefix, files) in reversed(self.data_files):
            if prefix.startswith(os.path.join(RELOCATE or '/', 'etc')):
                for basefile in files:
                    fn = os.path.join(prefix, os.path.basename(basefile))
                    if os.path.exists(fn):
                        print 'Skipping installation of %s (already exists)' % fn
                        files.remove(basefile)
            if not files:
                self.data_files.remove((prefix, []))


cmdclasses = {'install': InstallSGAS, 'install_data': InstallSGASData} 


setup(name='sgas-luts-service',
      version=__version__,
      description='SGAS LUTS Accounting Server',
      author='Magnus Jonsson',
      author_email='magnus@hpc2n.umu.se',
      url='http://www.sgas.se/',
      packages=['sgas', 'sgas/authz', 'sgas/database', 'sgas/database/postgresql',
                'sgas/ext', 'sgas/ext/isodate', 'sgas/ext/python',
                'sgas/queryengine', 'sgas/server', 'sgas/usagerecord', 
                'sgas/storagerecord', 'sgas/viewengine', 'sgas/generic'],

      cmdclass = cmdclasses,

      data_files=[
          ('share/sgas',                 ['datafiles/share/sgas.tac']),
          ('share/sgas/webfiles/css',    ['datafiles/share/webfiles/css/view.css']),
          ('share/sgas/webfiles/js',     ['datafiles/share/webfiles/js/protovis-r3.2.js',
                                          'datafiles/share/webfiles/js/protovis-helper.js']),
          ('share/sgas/postgres',        ['datafiles/share/postgresql/sgas-postgres-schema.sql',
                                          'datafiles/share/postgresql/sgas-postgres-view.sql',
                                          'datafiles/share/postgresql/sgas-postgres-functions.sql',
                                          'datafiles/share/postgresql/sgas-postgres-schema-drop.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.2-3.3-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.3-3.4-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.4-3.4.2-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.4.2-3.5-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.5.x-3.6.0-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.6.0-3.6.1-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.6.1-3.6.2-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.6.2-3.6.3-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.6.3-3.7.0-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-3.7.1-3.7.2-upgrade.sql',
                                          'datafiles/share/postgresql/sgas-postgres-aggregation-rebuild.sql',
                                          'datafiles/share/postgresql/sgas-postgres-cluster.sql']),
          ('/etc/',                      ['datafiles/etc/sgas.conf']),
          ('/etc/',                      ['datafiles/etc/sgas.authz']),
          ('/etc/init.d',                ['datafiles/etc/sgas']),
          ('/etc/nginx/sites-available', ['datafiles/etc/nginx/sites-available/sgas'])
      ]

)


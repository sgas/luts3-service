import os
from distutils.core import setup
from distutils.command.install_data import install_data


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


setup(name='sgas-accounting-server',
      version='3.0.0-git',
      description='SGAS Accounting Server',
      author='Henrik Thostrup Jensen',
      author_email='htj@ndgf.org',
      url='http://www.sgas.se/',
      packages=['sgas', 'sgas/server', 'sgas/ext', 'sgas/ext/isodate'],

      cmdclass = cmdclasses,

      data_files=[
          ('share/sgas',               ['datafiles/share/sgas.tac']),
          ('share/sgas/webfiles/css',  ['datafiles/share/webfiles/css/view.visualize.css']),
          ('share/sgas/webfiles/js',   ['datafiles/share/webfiles/js/jquery-1.3.2.min.js',
                                        'datafiles/share/webfiles/js/visualize.jQuery.js']),
          ('/etc/init.d',              ['datafiles/etc/sgas']),
          ('/etc/',                    ['datafiles/etc/sgas.conf']),
          ('/etc/',                    ['datafiles/etc/sgas.authz'])
      ]

)


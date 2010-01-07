"""
SSL context setup.
"""

import os

from OpenSSL import SSL



DEFAULT_HOST_KEY  = '/etc/grid-security/hostkey.pem'
DEFAULT_HOST_CERT = '/etc/grid-security/hostcert.pem'
DEFAULT_CERTIFICATES = '/etc/grid-security/certificates'



class ContextFactory:

    def __init__(self, key_path=DEFAULT_HOST_KEY, cert_path=DEFAULT_HOST_CERT,
                 verify=True, ca_dir=DEFAULT_CERTIFICATES):

        self.key_path = key_path
        self.cert_path = cert_path
        self.verify = verify
        self.ca_dir = ca_dir

        self.ctx = None


    def getContext(self):

        if self.ctx is not None:
            return self.ctx
        else:
            self.ctx = self._createContext()
            return self.ctx


    def _createContext(self):

        ctx = SSL.Context(SSL.SSLv23_METHOD) # this also allows tls 1.0
        ctx.set_options(SSL.OP_NO_SSLv2) # ssl2 is unsafe

        ctx.use_privatekey_file(self.key_path)
        ctx.use_certificate_file(self.cert_path)
        ctx.check_privatekey() # sanity check

        def verify_callback(conn, x509, error_number, error_depth, allowed):
            # just return what openssl thinks is right
            return allowed

        if self.verify:
            ctx.set_verify(SSL.VERIFY_PEER, verify_callback)

            calist = [ ca for ca in os.listdir(self.ca_dir) if ca.endswith('.0') ]
            for ca in calist:
                # openssl wants absolute paths
                ca = os.path.join(self.ca_dir, ca)
                ctx.load_verify_locations(ca)

        return ctx



if __name__ == '__main__':
    cf = ContextFactory()
    ctx = cf.getContext()
    print ctx


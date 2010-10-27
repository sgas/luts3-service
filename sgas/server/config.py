"""
Configuration utils.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009, 2010)
"""

import ConfigParser

# OrderedDict depends on the _abcall module, which is only
# available as of Python 2.6
try:
    from sgas.ext.python.collections import OrderedDict
except ImportError:
    OrderedDict = dict


# log isn't loaded yet, so make a fake log
class FakeLog:
    def msg(self, *args):
        print ','.join(args)
log = FakeLog()



# configuration constants
DEFAULT_HOSTKEY               = '/etc/grid-security/hostkey.pem'
DEFAULT_HOSTCERT              = '/etc/grid-security/hostcert.pem'
DEFAULT_CERTDIR               = '/etc/grid-security/certificates'
DEFAULT_AUTHZ_FILE            = '/etc/sgas.authz'
DEFAULT_HOSTNAME_CHECK_DEPTH  = '2'
DEFAULT_HOSTNAME_CHECK_WHITELIST = ''
DEFAULT_REVERSE_PROXY         = 'false'

# server options
SERVER_BLOCK         = 'server'
HOSTKEY              = 'hostkey'
HOSTCERT             = 'hostcert'
CERTDIR              = 'certdir'
DB                   = 'db'
AUTHZ_FILE           = 'authzfile'
HOSTNAME_CHECK_DEPTH = 'check_depth'
HOSTNAME_CHECK_WHITELIST = 'check_whitelist' # deprecated, still used to issue warnings
REVERSE_PROXY        = 'reverse_proxy'

# view options
VIEW_PREFIX      = 'view:'
VIEW_GROUP       = 'viewgroup'
VIEW_TYPE        = 'type'
VIEW_QUERY       = 'query'
VIEW_DESCRIPTION = 'description'
VIEW_DRAWTABLE   = 'drawtable'
VIEW_DRAWGRAPH   = 'drawgraph'



class ConfigurationError(Exception):
    pass



def readConfig(filename):

    # the dict_type option isn't supported until 2.5
    try:
        cfg = ConfigParser.SafeConfigParser(dict_type=OrderedDict)
    except TypeError:
        cfg = ConfigParser.SafeConfigParser()

    # add defaults
    cfg.add_section(SERVER_BLOCK)
    cfg.set(SERVER_BLOCK, HOSTKEY,              DEFAULT_HOSTKEY)
    cfg.set(SERVER_BLOCK, HOSTCERT,             DEFAULT_HOSTCERT)
    cfg.set(SERVER_BLOCK, CERTDIR,              DEFAULT_CERTDIR)
    cfg.set(SERVER_BLOCK, AUTHZ_FILE,           DEFAULT_AUTHZ_FILE)
    cfg.set(SERVER_BLOCK, HOSTNAME_CHECK_DEPTH, DEFAULT_HOSTNAME_CHECK_DEPTH)
    cfg.set(SERVER_BLOCK, HOSTNAME_CHECK_WHITELIST, DEFAULT_HOSTNAME_CHECK_WHITELIST)
    cfg.set(SERVER_BLOCK, REVERSE_PROXY,        DEFAULT_REVERSE_PROXY)

    fp = open(filename)
    proxy_fp = MultiLineFileReader(fp)

    # read cfg file
    cfg.readfp(proxy_fp)

    return cfg



class MultiLineFileReader:
    # implements the readline call for lines broken with \
    # readline is the only method called by configparser
    # so this is enough

    def __init__(self, fp):
        self._fp = fp

    def readline(self):

        line = self._fp.readline()

        while line.endswith('\\\n') or line.endswith('\\ \n'):
            if line.endswith('\\\n')  : i = -2
            if line.endswith('\\ \n') : i = -3

            newline = self._fp.readline()
            while newline.startswith('  '):
                newline = newline[1:]

            line = line[:i] + newline

        return line


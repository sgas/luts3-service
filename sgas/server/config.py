"""
Configuration utils.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009, 2010)
"""

import ConfigParser

import re

from sgas.ext.python import ConfigDict



# configuration defaults
DEFAULT_AUTHZ_FILE            = '/etc/sgas.authz'
DEFAULT_HOSTNAME_CHECK_DEPTH  = '2'

# server options
SERVER_BLOCK         = 'server'
DB                   = 'db'
AUTHZ_FILE           = 'authzfile'
HOSTNAME_CHECK_DEPTH = 'check_depth'

# the following are no longer used, but are used to issue warnings
HOSTKEY              = 'hostkey'
HOSTCERT             = 'hostcert'
CERTDIR              = 'certdir'
REVERSE_PROXY        = 'reverse_proxy'
HOSTNAME_CHECK_WHITELIST = 'check_whitelist'

# plugins
PLUGINS          = 'plugins'
PLUGIN_CLASS     = 'class'
PLUGIN_TYPE      = 'type'


class ConfigurationError(Exception):
    pass



def readConfig(filename):

    # the dict_type option isn't supported until 2.5
    try:
        cfg = ConfigParser.SafeConfigParser(dict_type=ConfigDict)
    except TypeError:
        cfg = ConfigParser.SafeConfigParser()

    # add defaults
    cfg.add_section(SERVER_BLOCK)
    cfg.set(SERVER_BLOCK, AUTHZ_FILE,           DEFAULT_AUTHZ_FILE)
    cfg.set(SERVER_BLOCK, HOSTNAME_CHECK_DEPTH, DEFAULT_HOSTNAME_CHECK_DEPTH)

    fp = open(filename)
    proxy_fp = MultiLineFileReader(fp)

    # read cfg file
    cfg.readfp(proxy_fp)

    return cfg



class MultiLineFileReader:
    # implements the readline call for lines broken with \
    # readline is the only method called by configparser
    # so this is enough
    # Also implements blocks for large queries
    # If the option is "<<<" the parser will read until a
    # line starting with "<<<" appears
    # An exception is raised if

    def __init__(self, fp):
        self._fp = fp

    def readline(self):

        line = self._fp.readline()

        # Multi line block
        if line.rstrip().endswith('=<<<') and not line.lstrip().startswith("#"):
            line = re.sub(r'<<<$',r'',line.rstrip())
            while True:
                cl = self._fp.readline().rstrip()
                if not cl:
                    raise ConfigurationError("ReadError: Reached end of file but found no <<<")
                if cl.startswith("<<<"):
                    break
                line += cl + " "
            return line.rstrip()

        # Multi line
        while line.endswith('\\\n') or line.endswith('\\ \n'):
            if line.endswith('\\\n')  : i = -2
            if line.endswith('\\ \n') : i = -3

            newline = self._fp.readline()
            while newline.startswith('  '):
                newline = newline[1:]

            line = line[:i] + newline

        return line





# OrderedDict depends on the _abcall module, which is only
# available as of Python 2.6
try:
    from sgas.ext.python.collections import OrderedDict as ConfigDict
except ImportError:
    ConfigDict = dict



# json module is only available from Python 2.6
# if we can't import it, try to import simplejson
try:
    import json
except ImportError:
    import simplejson as json



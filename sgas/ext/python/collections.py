#
# PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
#

# Source is originally from http://bugs.python.org/issue5397, patch od7.diff
# which was contributed by Raymond Hettinger <python at rcn.com>
#
# Copied to SGAS by Henrik Thostrup Jensen <htj@ndgf.org> on 6 Sep. 2010.
#
# This module depends on the _abcoll module which was added in Python 2.6
# Trying to import the module on an older module will cause an ImportError



from _abcoll import MutableMapping

# zip longest implementataion - does not exists in Python 2.6
# slightly modified to support 2.6 syntax (fillvalue paremeter removed)
# From: http://docs.python.org/release/3.0.1/library/itertools.html
def _zip_longest(*args):
    fillvalue = None
    # zip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
    def sentinel(counter = ([fillvalue]*(len(args)-1)).pop):
        yield counter()         # yields the fillvalue, or raises IndexError
    fillers = repeat(fillvalue)
    iters = [chain(it, sentinel(), fillers) for it in args]
    try:
        for tup in zip(*iters):
            yield tup
    except IndexError:
        pass



class OrderedDict(dict, MutableMapping):

    def __init__(self, *args, **kwds):
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        if not hasattr(self, '_keys'):
            self._keys = []
        self.update(*args, **kwds)

    def clear(self):
        del self._keys[:]
        dict.clear(self)

    def __setitem__(self, key, value):
        if key not in self:
            self._keys.append(key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._keys.remove(key)

    def __iter__(self):
        return iter(self._keys)

    def __reversed__(self):
        return reversed(self._keys)

    def popitem(self):
        if not self:
            raise KeyError('dictionary is empty')
        key = self._keys.pop()
        value = dict.pop(self, key)
        return key, value

    def __reduce__(self):
        items = [[k, self[k]] for k in self]
        inst_dict = vars(self).copy()
        inst_dict.pop('_keys', None)
        return (self.__class__, (items,), inst_dict)

    setdefault = MutableMapping.setdefault
    update = MutableMapping.update
    pop = MutableMapping.pop
    keys = MutableMapping.keys
    values = MutableMapping.values
    items = MutableMapping.items

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self.items()))

    def copy(self):
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        if isinstance(other, OrderedDict):
            return all(p==q for p, q in  _zip_longest(self.items(), other.items()))
        return dict.__eq__(self, other)


#!/usr/bin/python2.7
#

import argparse
import re
import sys
from psycopg2.extras import DateTimeRange
from datetime import datetime

# 
# B_BOUND [DateTime] ',' [DateTime] E_BOUND
#
# B_BOUND = '(' | '['
# E_BOUND = ')' | ']'
# DateTime = YYYY-MM-DD [HH:mm:ss]   [0-9]{4}-[0-1][0-9]-[0-3][0-9]( [0-2][0-9]:[0-6][0-9]:[0-6][0-9])?
#

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

DATETIME_RE = re.compile("[0-9]{4}-[0-1][0-9]-[0-3][0-9]( [0-2][0-9]:[0-6][0-9]:[0-6][0-9])?")
B_BOUND = ('(', '[')
E_BOUND = (')', ']')
DELIM = (',')


def parse_datetime(s):
    try:
        dt = datetime.strptime(s, DATETIME_FORMAT)
    except ValueError:
        dt = datetime.strptime(s, DATE_FORMAT)

    return dt


def tuple2str(t, conjunction="or"):
    if len(t) == 0:
        return ''
    else:
        s = "'%s'" % str(t[0])
        i = 1
        while i < len(t):
            if i == len(t) - 1:
                s += " %s '%s'" % (conjunction, str(t[i]))
            else:
                s += ", '%s'" % str(t[i])
            i += 1
        return s
    

class ParseError(Exception):
    pass


class Token():

    def __init__(self, s):
        self.s = s
        self.i = 0
        
    def next(self):
        whitespace = [' ', '\t']

        while self.i < len(self.s):

            if self.s[self.i] in whitespace:
                self.i += 1
                continue

            if self.s[self.i] in B_BOUND:
                c = self.s[self.i]
                self.i += 1
                return 'b_bound', c

            elif self.s[self.i] in E_BOUND:
                c = self.s[self.i]
                self.i += 1
                return 'e_bound', c

            elif self.s[self.i] in DELIM:
                c = self.s[self.i]
                self.i += 1
                return 'delim', c

            else:
                m = DATETIME_RE.match(self.s[self.i:])
                if m:
                    t = m.group(0)
                    self.i += len(t)
                    return 'datetime', t
                else:
                    return 'unrecognized', self.s[self.i:].split()[0]
        return None, ''


def parse_datetimerange(s):
    
    tok = Token(s)
    t,x = tok.next()
    if t != 'b_bound':
        raise ParseError("Expected %s in the beginning; found '%s'" % (tuple2str(B_BOUND),x))

    bounds = x

    t,x = tok.next()
    if t not in ('delim', 'datetime'):
        raise ParseError("Expected datetime string or ',' after '%s'; found '%s'" % (bounds, x))

    if t == 'datetime':
        t1 = parse_datetime(x)
        t,x = tok.next()
        if t != 'delim':
            raise ParseError("Expected ',' after first datetime string; found '%s'" % x)
    else:
        t1 = None

    t,x = tok.next()
    if t not in ('datetime', 'e_bound'):
        raise ParseError("Expected datetime string or %s after ','; found '%s'" % (tuple2str(E_BOUND), x))

    if t == 'datetime':
        t2 = parse_datetime(x)
        t,x = tok.next()
    else:
        t2 = None

    if t != 'e_bound':
         raise ParseError("Expected %s at the end" % tuple2str(E_BOUND))

    bounds += x

    return DateTimeRange(t1, t2, bounds)

    

        
if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='Parse a string representinga SateTimeRange')
    argparser.add_argument('-r', '--timerange', help="Timerange in format  [|( [YYYY-MM-DD[ hh:mm:ss]], [YYYY-MM-DD[ hh:mm:ss] ]|), e.g. '( , 2018-10-11)' = until (but not including) 2018-10-11")
    args = argparser.parse_args()

    try:
        dtr = parse_datetimerange(args.timerange)
    except ParseError as e:
        print "Parsing DateTimeString '%s' failed: %s" % (args.timerange, e)
        sys.exit(1)

    print dtr

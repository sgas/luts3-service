"""
Various conversion rutines. Mainly to present views properly.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""

import urllib



def viewResultToRows(doc):
    return [ (e['key'], e['value']) for e in doc['rows'] ]


def formatValue(value):
    if type(value) in (tuple, list):
        return ' / '.join( [ str(e) for e in value ] )
    else:
        return str(value)


def rowsToHTMLTable(doc, caption=None, base_indent=8, indent=4):
    # assumes a document with a complex key of length 2 and a value
    # it is somewhat assumed that the value pairs in the key form a matrix

    row_set = set()
    col_set = set()
    matrix = {}

    for key, value in doc:
        if type(key) in (list, tuple):
            k1, k2 = key # only up 2 dimensions are supported
        else:
            k1, k2 = key, ''
        row_set.add(k1)
        col_set.add(k2)
        matrix[k1,k2] = value

    rows    = sorted(row_set)
    columns = sorted(col_set)

    for rn in rows:
        for cn in columns:
            matrix.setdefault((rn,cn), '')

    table = createHTMLTable(columns, rows, matrix, caption, base_indent, indent)
    return table


def createHTMLTable(column_names, row_names, matrix, caption=None, base_indent=8, indent=4):

    i0 = ' ' * base_indent
    i1 = ' ' * base_indent + ' ' * 1 * indent
    i2 = ' ' * base_indent + ' ' * 2 * indent
    i3 = ' ' * base_indent + ' ' * 3 * indent

    res = ''

    res += i0 + '<table>\n'
    if caption is not None:
        res += i1 + '<caption>' + caption + '</caption>\n'
    # headers
    res += i1 + '<thead>\n'
    res += i2 + '<tr>\n'
    res += i3 + '<td></td>\n'
    for cn in column_names:
        res += i3 + '<th>' + str(cn) + '</th>\n'
    res += i2 + '</tr>\n'
    res += i1 + '</thead>\n'

    # body
    res += i1 + '<tbody>\n'
    for rn in row_names:
        res += i2 + '<tr>\n'
        res += i3 + '<th>' + str(rn) + '</th>\n'
        for cn in column_names:
            res += i3 + '<td>' + formatValue(matrix[rn,cn]) + '</td>\n'
        res += i2 + '</tr>\n'
    res += i1 + '</tbody>\n'

    res += i0 + '</table>\n'

    return res



def createLinkedHTMLTableList(rows, prefix='', caption=None, base_indent=8, indent=4):

    i0 = ' ' * base_indent
    i1 = ' ' * base_indent + ' ' * 1 * indent
    i2 = ' ' * base_indent + ' ' * 2 * indent
    i3 = ' ' * base_indent + ' ' * 3 * indent

    res = ''

    res += i0 + '<table>\n'
    if caption is not None:
        res += i1 + '<caption>' + caption + '</caption>\n'

    # body
    res += i1 + '<tbody>\n'
    for r in rows:
        url = prefix + urllib.quote(r, safe='')
        res += i2 + '<tr>\n'
        res += i3 + '<th><a href=%(url)s>%(row)s</a></th>\n' % {'url': url, 'row' : r }
        res += i2 + '</tr>\n'
    res += i1 + '</tbody>\n'

    res += i0 + '</table>\n'

    return res


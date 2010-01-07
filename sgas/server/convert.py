"""
Various conversion rutines. Mainly to present views properly.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009)
"""



def viewResultToRows(doc):
    return [ (e['key'], e['value']) for e in doc['rows'] ]



def rowsToHTMLTable(doc, caption=None, base_indent=8, indent=4):
    # assumes a document with a complex key of length 2 and a value
    # it is somewhat assumed that the value pairs in the key form a matrix

#    hi = 1
#    hi =     flip and 0 or 1
#    bi = not flip and 0 or 1

    header_set = set()
    for (key, value) in doc:
        header_set.add(key[1])
    headers = sorted(header_set)

    key_set = set()
    matrix = {}
    for (k1, k2), value in doc:
        key_set.add(k1)
        matrix[k1,k2] = value
    keys = sorted(key_set)
    for pk in keys:
        for sk in headers:
            try:
                matrix[pk, sk]
            except KeyError, e:
                matrix[pk, sk] = ''

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
    for sk in headers:
        res += i3 + '<th>' + sk + '</th>\n'
    res += i2 + '</tr>\n'
    res += i1 + '</thead>\n'

    # body
    res += i1 + '<tbody>\n'
    for pk in keys:
        res += i2 + '<tr>\n'
        res += i3 + '<th>' + pk + '</th>\n'
        for sk in headers:
            res += i3 + '<td>' + str(matrix[pk,sk]) + '</th>\n'
        res += i2 + '</tr>\n'
    res += i1 + '</tbody>\n'

    res += i0 + '</table>\n'

    return res


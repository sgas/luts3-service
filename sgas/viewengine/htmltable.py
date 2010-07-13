"""
HTML Table functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""



def createHTMLTable(matrix, column_names, row_names, caption=None, base_indent=8, indent=4):
    """
    Givena a matrix, and a suitable column and row names, create an HTML table of the data
    values in the matrix.
    """
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
            res += i3 + '<td>' + formatValue(matrix.get((rn,cn), '')) + '</td>\n'
        res += i2 + '</tr>\n'
    res += i1 + '</tbody>\n'

    res += i0 + '</table>\n'

    return res



def formatValue(value):
    """
    Formats an arbitrarely value into a string, suitable for insertion into
    a table.
    """
    if type(value) in (tuple, list):
        return ' / '.join( [ str(e) for e in value ] )
    else:
        return str(value)


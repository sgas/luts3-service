"""
HTML Table functionality. Part of SGAS viewengine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2011)
"""


class StyledTableValue:

    def __init__(self, value, bold=False, underlined=False, double_underlined=False):
        self.value = value
        self.bold = bold
        self.underlined = underlined
        self.double_underlined = double_underlined

    def html(self):
        tag = str(self.value)
        if self.bold:
            tag = '<b>' + tag + '</b>'
        if self.underlined:
            tag = '<u>' + tag + '</u>'
        if self.double_underlined:
            tag = '<u style="border-bottom: 1px inset black;">' + tag + '</u>'
        return tag



def createHTMLTable(matrix, batches, groups, caption=None, base_indent=8, indent=4, column_labels=None, skip_base_column=False):
    """
    Givena a matrix, and a suitable column and row names, create an HTML table of the data
    values in the matrix.
    """
    if column_labels is None:
        column_labels = {}

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
    for b in batches:
        res += i3 + '<th>' + str(column_labels.get(b, b)) + '</th>\n'
    res += i2 + '</tr>\n'
    res += i1 + '</thead>\n'

    # body
    res += i1 + '<tbody>\n'
    for g in groups:
        res += i2 + '<tr>\n'
        if skip_base_column:
            res += i3 + '<th> </th>\n'
        else:
            res += i3 + '<th>' + str(g) + '</th>\n'
        for b in batches:
            res += i3 + '<td>' + formatValue(matrix.get((b,g), 0)) + '</td>\n'
        res += i2 + '</tr>\n'
    res += i1 + '</tbody>\n'

    res += i0 + '</table>\n'

    return res



def formatValue(value):
    """
    Formats a value into a string, suitable for insertion into an HTML table.
    """
    if value is None:
        return ''

    if type(value) in (tuple, list):
        return ' / '.join( [ str(e) for e in value ] )
    if isinstance(value, StyledTableValue):
        return value.html()
    else:
        return str(value)


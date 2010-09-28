"""
Data processing functionaliy.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2010)
"""


def createMatrix(rows):

    row_set = set()
    col_set = set()
    matrix = {}

    for col_value, row_value, value in rows:
        row_set.add(row_value)
        col_set.add(col_value)
        matrix[row_value,col_value] = value

    rowss   = sorted(row_set)
    columns = sorted(col_set)

    return matrix, columns, rowss



def createMatrixList(rows, row_name):

    col_set = set()
    matrix = {}

    for col_value, value in rows:
        col_set.add(col_value)
        matrix[row_name, col_value] = value

    columns = sorted(col_set)

    return matrix, columns



def linearizeBlanks(matrix, m_rows, m_columns):
    # set blank values to previous values in data series
    for mr in m_rows:
        blank_value = 0
        for mc in m_columns:
            if matrix.get((mr,mc)) is None:
                matrix[mr,mc] = blank_value
            else:
                blank_value = matrix[mr,mc]

    return matrix



def calculateStackedMaximum(matrix):

    stack_values = {}
    for (rn, cn), value in matrix.items():
        stack_values[cn] = stack_values.get(cn, 0) + value

    return max(stack_values.values() + [1])



def createJSMatrix(matrix, column_names, row_names, fill_value='0'):

    rows = []
    for rn in row_names:
        rows.append( '[' + ','.join( [ str(matrix.get((rn,cn), fill_value)) for cn in column_names ] ) + ']' )
    return ',\n  '.join(rows)



def createJSTransposedMatrix(matrix, column_names, row_names, fill_value='0'):

    rows = []
    for cn in column_names:
        rows.append( '[' + ','.join( [ str(matrix.get((rn,cn), fill_value)) for rn in row_names ] ) + ']' )
    return ',\n  '.join(rows)



def createJSList(matrix, column_names, row):

    elements = ','.join( [ str(matrix[row,cn]) or '0' for cn in column_names ] )
    return elements



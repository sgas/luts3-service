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



def calculateStackedMaximum(matrix):

    stack_values = {}
    for (rn, cn), value in matrix.items():
        stack_values[cn] = stack_values.get(cn, 0) + value

    return max(stack_values.values())



def createJSMatrix(matrix, column_names, row_names):

    rows = []
    for rn in row_names:
        rows.append( '\n[' + ','.join( [ str(matrix.get((rn,cn), '0')) for cn in column_names ] ) + ']' )
    return ','.join(rows)



def createJSList(matrix, column_names, row):

    elements = ','.join( [ str(matrix[row,cn]) or '0' for cn in column_names ] )
    return elements



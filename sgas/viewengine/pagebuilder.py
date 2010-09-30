"""
Page builder. High-level interface for building the HTML pages in view engine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from sgas.viewengine import dataprocess, graphbuilder, htmltable


DEFAULT_TABLE_RENDER_LIMIT = 15


def buildViewPage(view, rows):
    # note, just the body of the page is created, not the entire page

    # this scheme needs to be changed sometime (should be encapsulated into the graph builder)
    if view.view_type == 'lines':
        matrix, m_columns, m_rows = dataprocess.createMatrix(rows)

    elif view.view_type == 'columns':
        row_name = ''
        matrix, m_columns = dataprocess.createMatrixList(rows, row_name)
        m_rows = [row_name]

    elif view.view_type == 'stacked_columns':
        matrix, m_columns, m_rows = dataprocess.createMatrix(rows)

    elif view.view_type == 'grouped_columns':
        matrix, m_columns, m_rows = dataprocess.createMatrix(rows)

    elif view.view_type == 'discrete_scatterplot':
        matrix, m_columns, m_rows = dataprocess.createScatterMatrix(rows)

    else:
        raise AssertionError('Invalid view type specified (%s)' % view.view_type)

    render_table = shouldRenderTable(view, m_columns)
    render_graph = shouldRenderGraph(view)

    body = view.caption + '\n<p>\n'
    if render_table:
        table = htmltable.createHTMLTable(matrix, m_columns, m_rows)
        body += table

    if render_table and render_graph:
        body += '    <p> &nbsp; <p>\n'

    if render_graph:
        graph = graphbuilder.buildGraph(view.view_type, matrix, m_columns, m_rows)
        body += graph

    return body



def shouldRenderTable(view, rows):

    if view.drawtable is not None:
        return view.drawtable
    else:
        if len(rows) > DEFAULT_TABLE_RENDER_LIMIT:
            return False
        else:
            return True



def shouldRenderGraph(view):

    if view.drawgraph is not None:
        return view.drawtable
    else:
        return True


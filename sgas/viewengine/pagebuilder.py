"""
Page builder. High-level interface for building the HTML pages in view engine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from sgas.viewengine import dataprocess, graphbuilder, htmltable


DEFAULT_TABLE_RENDER_LIMIT = 15


def buildViewPage(view, rows):
    # note, just the body of the page is created, not the entire page

    if view.view_type == 'bar':
        row_name = ''
        matrix, m_columns = dataprocess.createMatrixList(rows, row_name)
        m_rows = [row_name]

    elif view.view_type == 'stacked_bars':
        matrix, m_columns, m_rows = dataprocess.createMatrix(rows)

    else:
        raise AssertionError('Invalid view type specified (%s)' % view.view_type)


    body = ''
    if shouldRenderTable(view, rows):
        table = htmltable.createHTMLTable(m_columns, m_rows, matrix, view.caption)
        body += table

    if shouldRenderGraph(view):
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


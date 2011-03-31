"""
Page builder. High-level interface for building the HTML pages in view engine.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""

from sgas.viewengine import viewcore, dataprocess, htmltable



def buildViewPage(view, rows):
    # note, just the body of the page is created, not the entire page

    batches, groups, dict_list = buildRowAggregates(rows, view)
    n_batches = len(batches) if batches else 0
    n_groups  = len(groups)  if groups  else 0

    render_table = shouldRenderTable(view, n_batches, n_groups)
    render_graph = shouldRenderGraph(view)

    body = view.caption + '\n<p>\n'
    if render_table:
        matrix = dataprocess.createMatrix(dict_list)
        table = htmltable.createHTMLTable(matrix, sorted(batches), sorted(groups or [0]))
        body += table

    if render_table and render_graph:
        body += '    <p> &nbsp; <p>\n'

    if render_graph:
        js_data = dataprocess.createJavascriptData(dict_list)
        graph = viewcore.VIEW_TYPES[view.view_type][viewcore.GRAPH_TEMPLATE] % {'data' : js_data}
        body += graph

    return body



def buildRowAggregates(rows, view):

    view_cfg = viewcore.VIEW_TYPES[view.view_type]

    batch_idx = view_cfg.get(viewcore.BATCH_IDX, None)
    group_idx = view_cfg.get(viewcore.GROUP_IDX, None)

    if batch_idx is not None:
        batches = sorted(dataprocess.uniqueEntries(rows, batch_idx))
    else:
        batches = None

    if group_idx is not None:
        groups = sorted(dataprocess.uniqueEntries(rows, group_idx))
    else:
        groups = None

    dict_list = dataprocess.buildDictList(rows, view_cfg[viewcore.DATA_TRANSFORM])

    return batches, groups, dict_list



def shouldRenderTable(view, n_batches, n_groups):

    view_cfg = viewcore.VIEW_TYPES[view.view_type]

    cfg_render = view_cfg.get(viewcore.RENDER_OVERRIDE, view.drawtable)
    if cfg_render is not None:
        return cfg_render

    if n_batches > viewcore.DEFAULT_BATCH_TABLE_RENDER_LIMIT:
        return False

    if n_groups > viewcore.DEFAULT_GROUP_TABLE_RENDER_LIMIT:
        return False

    return True



def shouldRenderGraph(view):

    if view.drawgraph is not None:
        return view.drawtable
    else:
        return True


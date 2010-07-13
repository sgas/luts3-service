"""
Functionality for building graphs.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""


from sgas.viewengine import dataprocess



DEFAULT_GRAPH_WIDTH  = 980
DEFAULT_GRAPH_HEIGTH = 450



JAVASCRIPT_BAR_GRAPH = """
     <script type="text/javascript+protovis">

var d = [%(data)s];

var w = %(width)i;
var h = %(height)i;

var x = pv.Scale.ordinal(pv.range(%(n_columns)i)).splitBanded(0, w-80, 4/5);
var y = pv.Scale.linear(0, %(bar_height)i).range(0, h);

var vis = new pv.Panel()
    .width(w)
    .height(h)
    .bottom(20)
    .left(50)
    .right(5)
    .top(5);

var bar = vis.add(pv.Bar)
    .data(d)
    .bottom(0)
    .width(x.range().band)
    .height(y)
    .left(function() x(this.index));

bar.anchor("bottom").add(pv.Label)
    .visible(function() !this.parent.index)
    .textMargin(8)
    .textBaseline("top")
    .text(function() %(columns)s[this.index]);

vis.add(pv.Rule)
    .data(y.ticks())
    .bottom(function(d) Math.round(y(d)) - .5 )
    .strokeStyle(function(d) d ? "rgba(255,255,255,.3)" : "#000")
   .add(pv.Rule)
    .left(0)
    .width(0)
    .strokeStyle("#000")
    .anchor("left").add(pv.Label)
    .text(function(d) parseInt(d.toFixed(1)));

vis.render();

    </script>
"""



JAVASCRIPT_STACKED_BAR_GRAPH = """
    <script type="text/javascript+protovis">

c = pv.Colors.category20();
var d = [
    %(data)s
];

var data = d,
    w = %(width)i,
    h = %(height)i,
    x = pv.Scale.ordinal(pv.range(%(n_columns)i)).splitBanded(0, w-80, 4/5),
    y = pv.Scale.linear(0, %(bar_height)i).range(0, h);

var vis = new pv.Panel()
    .width(w)
    .height(h)
    .bottom(20)
    .left(50)
    .right(5)
    .top(5);

var bar = vis.add(pv.Panel)
    .data(data)
    .add(pv.Bar)
    .data(function(a) a)
    .left(function() x(this.index))
    .width(x.range().band)
    .bottom(pv.Layout.stack())
    .height(y);

bar.anchor("bottom").add(pv.Label)
    .visible(function() !this.parent.index)
    .textMargin(8)
    .textBaseline("top")
    .text(function() %(columns)s[this.index]);

/* y-scale and ticks */
vis.add(pv.Rule)
    .data(y.ticks())
    .bottom(function(d) Math.round(y(d)) - .5)
    .strokeStyle(function(d) d ? "rgba(255,255,255,.0)" : "#000")
    .add(pv.Rule)
    .left(0)
    .width(0)
    .strokeStyle("#000")
    .anchor("left").add(pv.Label)
    .text(function(d) parseInt(d.toFixed(1)));

/* legend */
vis.add(pv.Dot)
    .data(%(stacks)s)
    .right(10)
    .top(function() 5 + this.index * 18)
    .size(40)
    .strokeStyle('#444444')
    .fillStyle(function(d) c(this.index) )
    .anchor("left")
    .add(pv.Label);

vis.render();

    </script>
"""




def buildGraph(view_type, matrix, m_columns, m_rows=None):

    if view_type == 'bars':

        assert len(m_rows) == 1, 'Only one row allowed in bar matrix.'
        data = dataprocess.createJSList(matrix, m_columns, m_rows[0])
        cols = _createColumnNames(m_columns)
        maximum = max(matrix.values())
        bar_height = int(maximum*1.02)

        graph_args = {
            'data' : data, 'width': DEFAULT_GRAPH_WIDTH, 'height': DEFAULT_GRAPH_HEIGTH,
            'n_columns': len(m_columns), 'columns': cols, 'bar_height': bar_height
        }
        return JAVASCRIPT_BAR_GRAPH % graph_args


    elif view_type == 'stacked_bars':

        data = dataprocess.createJSMatrix(matrix, m_columns, m_rows)
        cols = _createColumnNames(m_columns)
        maximum = dataprocess.calculateStackedMaximum(matrix)
        bar_height = int(maximum*1.02)

        graph_args = {
            'data': data, 'width': DEFAULT_GRAPH_WIDTH, 'height': DEFAULT_GRAPH_HEIGTH,
            'bar_height':bar_height, 'n_columns': len(m_columns), 'columns':cols, 'stacks':m_rows
        }
        return JAVASCRIPT_STACKED_BAR_GRAPH % graph_args


    else:
        raise AssertionError('Invalid view type specified (%s)' % view_type)



def _createColumnNames(m_columns):

    n_columns = len(m_columns)

    if n_columns<= 12:
        return m_columns

    if n_columns <= 20:
        # every other element
        return [ m_columns[i] if i % 2 == 0 else '' for i in range(0, len(m_columns))]

    # just assume dates for now
    if n_columns < 100:
        return [ c if c.endswith('01') or c.endswith('15') else '' for c in m_columns ]

    return [ c if c.endswith('01') else '' for c in m_columns ]


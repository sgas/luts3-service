"""
Functionality for building graphs.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2010)
"""


from sgas.viewengine import dataprocess



DEFAULT_GRAPH_WIDTH  = 1020
DEFAULT_GRAPH_HEIGTH = 460



JAVASCRIPT_LINE_GRAPH = """
    <script type="text/javascript+protovis">

var c = pv.Colors.category20();

var data = [
    %(data)s
];

var w = %(width)i;
var h = %(height)i;

var x = pv.Scale.ordinal(pv.range(%(n_columns)i)).split(0, w-120);
var y = pv.Scale.linear(0, %(graph_heigth)i).range(0, h);


var vis = new pv.Panel()
    .width(w)
    .height(h)
    .bottom(20)
    .left(50)
    .right(5)
    .top(5);

/* x-axis ticks */
vis.add(pv.Panel)
    .data(%(x_markers)s)
  .add(pv.Label)
    .textBaseline("top")
    .bottom(-2)
    .left(function(d) x(this.parent.index))
    .text(function(d) d);

/* y ticks */
vis.add(pv.Rule)
    .data(y.ticks())
    .bottom(y)
    .width(w-120)
    .strokeStyle(function(d) d ? "#ccc" : "#000")
  .anchor("left").add(pv.Label)
    .textStyle(function() i > 0 ? "#000" : "#000")
    .text(y.tickFormat);


/* the lines */
vis.add(pv.Panel)
    .data(data)
  .add(pv.Line)
    .data(function(d) d)
    .left(function(d) x(this.index))
    .bottom(function(d) y(d))
    .strokeStyle(function(d) c(this.parent.index))
    .lineWidth(3)

/* legend */
vis.add(pv.Dot)
    .data(%(groups)s)
    .right(10)
    .top(function() (5 + (%(n_groups)i  - this.index) * 19))
    .size(40)
    .strokeStyle('#444444')
    .fillStyle(function(d) c(this.index) )
    .anchor("left")
    .add(pv.Label);

vis.render();

    </script>
"""



JAVASCRIPT_COLUMN_GRAPH = """
     <script type="text/javascript+protovis">

var d = [%(data)s];

var w = %(width)i;
var h = %(height)i;

var x = pv.Scale.ordinal(pv.range(%(n_columns)i)).splitBanded(0, w, 4/5);
var y = pv.Scale.linear(0, %(column_height)i).range(0, h);

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



JAVASCRIPT_STACKED_COLUMN_GRAPH = """
    <script type="text/javascript+protovis">

c = pv.Colors.category20();
var d = [
    %(data)s
];

var data = d,
    w = %(width)i,
    h = %(height)i,
    x = pv.Scale.ordinal(pv.range(%(n_columns)i)).splitBanded(0, w-100, 4/5),
    y = pv.Scale.linear(0, %(column_height)i).range(0, h);

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
    .width(w-75)
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
    .top(function() (5 + (%(n_stacks)i  - this.index) * 19))
    .size(40)
    .strokeStyle('#444444')
    .fillStyle(function(d) c(this.index) )
    .anchor("left")
    .add(pv.Label);

vis.render();

    </script>
"""



JAVASCRIPT_GROUPED_COLUMN_GRAPH = """
    <script type="text/javascript+protovis">

var data = [
    %(data)s
];

var n = %(n_groups)i;
var m = %(n_columns)i;

/* Sizing and scales. */
var w = %(width)s;
var h = %(height)s;
var x = pv.Scale.ordinal(pv.range(n)).splitBanded(0, w-100, 0.8);
var y = pv.Scale.linear(0, %(column_height)i).range(0, h);


/* The root panel */
var vis = new pv.Panel()
    .width(w)
    .height(h)
    .bottom(20)
    .left(50)
    .right(5)
    .top(5);


/* The columns */
var bar = vis.add(pv.Panel)
    .data(data)
    .left(function() x(this.index))
  .add(pv.Bar)
    .data(function(a) a)
    .left(function() this.index * x.range().band / m)
    .width(x.range().band / m)
    .bottom(0)
    .height(y)
    .fillStyle(pv.Colors.category20().by(pv.index));


/* X-axis group labels */
vis.add(pv.Label)
    .data(pv.range(n))
    .bottom(0)
    .left(function() x(this.index) + x.range().band * 0.45)
    .textMargin(5)
    .textBaseline("top")
    .text(function() %(group_names)s[this.index]);


/* Y-axis labels and ticks */
vis.add(pv.Rule)
    .data(y.ticks(10))
    .bottom(function(d) Math.round(y(d)) - .5 )
    .strokeStyle(function(d) d ? "rgba(255,255,255,.2)" : "#000")
  .add(pv.Rule)
    .left(0)
    .width(0) // axis tick length
    .strokeStyle("#000")
  .anchor("left").add(pv.Label)
    .text(function(d) parseInt(d.toFixed(1)));


/* Column color legend */
vis.add(pv.Dot)
    .data(%(column_names)s)
    .right(10)
    .top(function() 5 + this.index * 19)
    .size(50)
    .strokeStyle('#444444')
    .fillStyle(pv.Colors.category20().by(pv.index))
    .anchor("left")
    .add(pv.Label);


vis.render();

    </script>
"""



def buildGraph(view_type, matrix, m_columns, m_rows=None):

    if view_type == 'lines':
        # m_columns -> time series, m_rows -> groups

        # set blank values to previous values in data series
        matrix = dataprocess.linearizeBlanks(matrix, m_rows, m_columns)
        data = dataprocess.createJSMatrix(matrix, m_columns, m_rows)
        x_markers = _createColumnNames(m_columns)
        maximum = max(matrix.values() + [1])

        graph_args = {
            'data': data, 'width': DEFAULT_GRAPH_WIDTH, 'height': DEFAULT_GRAPH_HEIGTH,
            'graph_heigth': maximum, 'x_markers': x_markers, 'n_columns': len(m_columns),
            'groups': m_rows, 'n_groups': len(m_rows)
        }
        return JAVASCRIPT_LINE_GRAPH % graph_args


    elif view_type == 'columns':

        assert len(m_rows) == 1, 'Only one row allowed in column matrix.'
        data = dataprocess.createJSList(matrix, m_columns, m_rows[0])
        cols = _createColumnNames(m_columns)
        maximum = max(matrix.values() + [1])
        column_height = int(maximum*1.02)

        graph_args = {
            'data' : data, 'width': DEFAULT_GRAPH_WIDTH, 'height': DEFAULT_GRAPH_HEIGTH,
            'n_columns': len(m_columns), 'columns': cols, 'column_height': column_height
        }
        return JAVASCRIPT_COLUMN_GRAPH % graph_args


    elif view_type == 'stacked_columns':

        data = dataprocess.createJSMatrix(matrix, m_columns, m_rows)
        cols = _createColumnNames(m_columns)
        maximum = dataprocess.calculateStackedMaximum(matrix)
        column_height = int(maximum*1.02)

        graph_args = {
            'data': data, 'width': DEFAULT_GRAPH_WIDTH, 'height': DEFAULT_GRAPH_HEIGTH,
            'column_height': column_height, 'n_columns': len(m_columns), 'columns': cols,
            'n_stacks': len(m_rows), 'stacks':m_rows
        }
        return JAVASCRIPT_STACKED_COLUMN_GRAPH % graph_args

    elif view_type == 'grouped_columns':

        data = dataprocess.createJSTransposedMatrix(matrix, m_columns, m_rows, fill_value='undefined')
        cols = _createColumnNames(m_columns)
        maximum = max(matrix.values() + [1])
        column_height = int(maximum*1.02)

        graph_args = {
            'data': data, 'width': DEFAULT_GRAPH_WIDTH, 'height': DEFAULT_GRAPH_HEIGTH,
            'n_groups':len(m_columns), 'n_columns':len(m_rows), 'column_height':column_height,
            'group_names': m_columns, 'column_names':m_rows
        }
        return JAVASCRIPT_GROUPED_COLUMN_GRAPH % graph_args

    else:
        raise AssertionError('Invalid view type specified (%s)' % view_type)



def _createColumnNames(m_columns):

    n_columns = len(m_columns)

    if n_columns <= 12:
        return m_columns

    if n_columns <= 20:
        # every other element
        return [ m_columns[i] if i % 2 == 0 else '' for i in range(0, len(m_columns))]

    # just assume dates for now
    if n_columns <= 50:
        return [ c if c.endswith('01') or c.endswith('10') or c.endswith('20') else '' for c in m_columns ]

    if n_columns < 100:
        return [ c if c.endswith('01') or c.endswith('15') else '' for c in m_columns ]

    return [ c if c.endswith('01') else '' for c in m_columns ]


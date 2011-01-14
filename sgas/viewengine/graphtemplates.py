"""
Templates for Protovis Javascript graphs.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


TEMPLATE_COLUMNS = """
    <script type="text/javascript+protovis">

var data = %(data)s;

var batches = pv.uniq(data, function(d) d.batch).sort();

var x_ticks = create_xticks(batches);

var count = pv.nest(data)
    .key(function(d) d.batch)
    .rollup(function(v) pv.sum(v, function(d) d.value));

var y_max = pv.max(data.map(function(d) d.value));

var w = window.innerWidth * 0.9;
var h = window.innerHeight * 0.65;
var x = pv.Scale.ordinal(batches).splitBanded(0, w, 0.8);
var y = pv.Scale.linear(0, y_max).range(0, h);

var c = pv.Colors.category20();

vis = create_panel(w,h);

add_xticks(vis, x, x_ticks);

add_yticks(vis, y, w);

/* bars */
vis.add(pv.Panel)
    .data(batches)
    .left(x)
  .add(pv.Bar)
    .width(x.range().band)
    .bottom(0)
    .height(function(d) y(count[d]))
    .fillStyle(function(d) c(0));

vis.render();
    </script>

"""


TEMPLATE_STACKED_COLUMNS = """
<script type="text/javascript+protovis">

var data = %(data)s;

var batches = pv.uniq(data, function(d) d.batch).sort()
var groups = pv.uniq(data, function(d) d.group).sort().reverse();

x_ticks = create_xticks(batches);

var count = pv.nest(data)
    .key(function(d) d.batch)
    .key(function(d) d.group)
    .rollup(function(v) v[0].value);

var values = {};

for (bi in batches) {
    b = batches[bi];
    values[b] = {};
    for (gi in groups) {
        g = groups[gi];
        values[b][g] = 0;
        if (count[b] != undefined && count[b][g] != undefined) {
            values[b][g] = count[b][g];
        }
    }
}

var sum_batches = pv.nest(data)
    .key(function(d) d.batch)
    .rollup(function(v) pv.sum(v, function(d) d.value));

var y_max = pv.max(pv.values(sum_batches)) * 1.03;

var w = window.innerWidth * 0.9;
var h = window.innerHeight * 0.65;
var x = pv.Scale.ordinal(batches).splitBanded(0, w-150, 0.8);
var y = pv.Scale.linear(0, y_max).range(0, h);
var c = pv.Colors.category20();

vis = create_panel(w,h);

add_xticks(vis, x, x_ticks);

add_yticks(vis, y, w-150);

add_legend(vis, groups);

/* bars */
vis.add(pv.Panel)
    .data(batches)
    .left(x)
  .add(pv.Layout.Stack)
    .layers(groups)
    .values(function(g,b) [ values[b][g] ] )
    .y(function(d) y(d))
  .layer.add(pv.Bar)
    .width(x.range().band);

vis.render();
    </script>
"""


TEMPLATE_GROUPED_COLUMNS = """
<script type="text/javascript+protovis">

var data = %(data)s;

var batches = pv.uniq(data, function(d) d.batch).sort()
var groups = pv.uniq(data, function(d) d.group).sort().reverse();

var x_ticks = create_xticks(batches);

var count = pv.nest(data)
    .key(function(d) d.batch)
    .key(function(d) d.group)
    .rollup(function(v) v[0].value);

var y_max = pv.max(data.map(function(d) d.value)) * 1.03;

var w = window.innerWidth * 0.9;
var h = window.innerHeight * 0.65;
var x = pv.Scale.ordinal(batches).splitBanded(0, w-150, 0.8);
var y = pv.Scale.linear(0, y_max).range(0, h);
var xd = pv.Scale.ordinal(groups).splitBanded(0, x.range().band, 0.95);

var c = pv.Colors.category20();

vis = create_panel(w, h);

add_xticks(vis, x, x_ticks);

add_yticks(vis, y, w-150);

add_legend(vis, groups);

/* bars */
vis.add(pv.Panel)
    .data(batches)
    .left(x)
  .add(pv.Bar)
    .data(groups)
    .width(xd.range().band)
    .bottom(0)
    .left(xd)
    .height(function(group, batch) y(count[batch][group]))
    .fillStyle(function(group, batch) c(group));

vis.render();
    </script>
"""


TEMPLATE_LINES = """
<script type="text/javascript+protovis">

var data = %(data)s;

var batches = pv.uniq(data, function(d) d.batch).sort();
var groups = pv.uniq(data, function(d) d.group).sort().reverse();

var x_ticks = create_xticks(batches);

var count = pv.nest(data)
    .key(function(d) d.batch)
    .key(function(d) d.group)
    .rollup(function(v) v[0].value);

var y_max = pv.max(data.map(function(d) d.value));

var w = window.innerWidth * 0.9;
var h = window.innerHeight * 0.65;
var x = pv.Scale.ordinal(batches).splitBanded(0, w-150, 0.8);
var y = pv.Scale.linear(0, y_max).range(0, h);
var c = pv.Colors.category20();

var vis = create_panel(w, h);

add_xticks(vis, x, x_ticks);

add_yticks(vis, y, w-150);

add_legend(vis, groups);

/* lines */
vis.add(pv.Panel)
    .data(groups)
  .add(pv.Line)
    .data(batches)
    .left(function(b, g) x(b))
    .bottom(function(b, g) y(count[b][g]))
    .strokeStyle(function(b, g) c(g))
    .interpolate('basis')
    .lineWidth(3);

vis.render();
    </script>
"""


TEMPLATE_SCATTERPLOT = """
<script type="text/javascript+protovis">

var data = %(data)s;

var groups = pv.uniq(data, function(d) d.group).sort().reverse();

var x_values = data.map(function(d) d.x);
var y_values = data.map(function(d) d.y);
var z_values = data.map(function(d) d.z);

var x_max = pv.max(pv.values(x_values)) * 1.03;
var y_max = pv.max(pv.values(y_values)) * 1.03;
var z_max = pv.max(pv.values(z_values)) * 1.03;

var w = window.innerWidth * 0.9;
var h = window.innerHeight * 0.65;
var x = pv.Scale.linear(0, x_max).range(0, w-150);
var y = pv.Scale.linear(0, y_max).range(0, h);
var z = pv.Scale.root(0, z_max).range(0, 400);
var c = pv.Colors.category20();

vis = create_panel(w, h);

/* x-ticks */
vis.add(pv.Rule)
    .data(x.ticks(10))
    .left(x)
    .strokeStyle(function(d) d ? "#eee" : "#000")
  .anchor("bottom").add(pv.Label)
    .visible(function(d) d > 0)
    .text(x.tickFormat);

/* y-ticks */
vis.add(pv.Rule)
    .data(y.ticks(10))
    .bottom(y)
    .width(w-150)
    .strokeStyle(function(d) d ? "#eee" : "#000")
  .anchor("left")
  .add(pv.Label)
    .visible(function(d) d > 0)
    .text(y.tickFormat);

add_legend(vis, groups);

/* dots */
vis.add(pv.Panel)
    .data(data)
  .add(pv.Dot)
    .left(function(d) x(d.x))
    .bottom(function(d) y(d.y))
    .size(function(d) z(d.z))
    .strokeStyle(function(d) c(d.group))
    .fillStyle(function() this.strokeStyle().alpha(.2));

vis.render();
    </script>
"""


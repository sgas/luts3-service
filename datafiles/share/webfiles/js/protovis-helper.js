/*
 * Various helper functions for dealing with protovis.
 *
 * Author: Henrik Thostrup Jensen <htj@ndgf.org>
 */


function create_panel(width, heigth) {

    return new pv.Panel()
        .width(width)
        .height(heigth)
        .bottom(20)
        .left(50)
        .right(5)
        .top(5);

}


function add_xticks(vis, x, x_ticks) {

    vis.add(pv.Label)
        .data(x_ticks)
        .bottom(0)
        .left(function(d) { return x(d) + x.range().band / 2; } )
        .textMargin(5)
        .textBaseline("top")
        .textAlign("center")
        .text(function(d) { return d; } );

    vis.add(pv.Rule)
        .strokeStyle("rgba(000, 000, 000, 1)")
        .left(0)
        .bottom(-1);

}


function add_yticks(vis, y, width) {

    vis.add(pv.Rule)
        .data(y.ticks())
        .strokeStyle("rgba(000, 000, 000, .05)")
        .bottom(y)
        .width(width)
        .anchor("left")
    .add(pv.Label)
        .text(y.tickFormat);

    vis.add(pv.Rule)
        .strokeStyle("rgba(000, 000, 000, 1)")
        .bottom(-0.5)
        .width(width)
        .anchor("left");

}


function add_legend(vis, groups) {

    vis.add(pv.Dot)
        .data(groups)
        .right(10)
        .top(function() { return (5 + (groups.length  - this.index) * 19); } )
        .size(40)
        .strokeStyle('#444444')
        .fillStyle(function(g) { return c(g); } )
        .anchor("left")
      .add(pv.Label);

}


function create_xticks(batches) {

    // It is assumed that batches is an array of dates in string format

    if (batches.length <= 12) {
        return batches;
    }

    x_ticks = [];

    if (batches.length <= 20) {
        for (var i = 0; i < batches.length; i += 2) {
            x_ticks.push(batches[i]);
        }
    }
    else if (batches.length <= 50) {
        for (var i = 0; i < batches.length; i++) {
            if (batches[i].substr(-2) == '01' || batches[i].substr(-2) == '15') {
                x_ticks.push(batches[i]);
            }
        }
    }
    else {
        for (var i = 0; i < batches.length; i++) {
            if (batches[i].substr(-2) == '01') {
                x_ticks.push(batches[i]);
            }
        }
    }

    return x_ticks;

}


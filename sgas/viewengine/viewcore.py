"""
Core view configuration.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2009-2011)
"""


from sgas.viewengine import graphtemplates


# view types
VIEW_LINES              = 'lines'
VIEW_COLUMNS            = 'columns'
VIEW_STACKED_COLUMNS    = 'stacked_columns'
VIEW_GROUPED_COLUMNS    = 'grouped_columns'
VIEW_SCATTERPLOT        = 'scatterplot'

# config constants
DATA_TRANSFORM  = 'key_transform'
BATCH_IDX       = 'batch_idx'
GROUP_IDX       = 'group_idx'
RENDER_OVERRIDE = 'render_override'
GRAPH_TEMPLATE  = 'graph_template'

# entity types
BATCH = 'batch'
GROUP = 'group'
VALUE = 'value'
X     = 'x'
Y     = 'y'
Z     = 'z'

# limits for table rendering
DEFAULT_BATCH_TABLE_RENDER_LIMIT = 15
DEFAULT_GROUP_TABLE_RENDER_LIMIT = 25

VIEW_TYPES = {
    VIEW_COLUMNS : {
        DATA_TRANSFORM : [ BATCH, VALUE ],
        BATCH_IDX : 0,
        GRAPH_TEMPLATE : graphtemplates.TEMPLATE_COLUMNS
    },
    VIEW_STACKED_COLUMNS : {
        DATA_TRANSFORM : [ BATCH, GROUP, VALUE ],
        BATCH_IDX : 0,
        GROUP_IDX : 1,
        GRAPH_TEMPLATE : graphtemplates.TEMPLATE_STACKED_COLUMNS
    },
    VIEW_GROUPED_COLUMNS : {
        DATA_TRANSFORM : [ BATCH, GROUP, VALUE ],
        BATCH_IDX : 0,
        GROUP_IDX : 1,
        GRAPH_TEMPLATE : graphtemplates.TEMPLATE_GROUPED_COLUMNS
    },
    VIEW_LINES : {
        DATA_TRANSFORM : [ BATCH, GROUP, VALUE ],
        BATCH_IDX : 0,
        GROUP_IDX : 1,
        GRAPH_TEMPLATE : graphtemplates.TEMPLATE_LINES
    },
    VIEW_SCATTERPLOT : {
        DATA_TRANSFORM : [ GROUP, X, Y, Z ],
        BATCH_IDX : 0,
        GROUP_IDX : 1,
        RENDER_OVERRIDE : False, # never render scatterplot data in table
        GRAPH_TEMPLATE : graphtemplates.TEMPLATE_SCATTERPLOT
    }
}


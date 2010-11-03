"""
Authorization rights.
"""


# actions
ACTION_INSERT    = 'insert'
ACTION_VIEW      = 'view'
ACTION_QUERY     = 'query'

ACTIONS = [ ACTION_INSERT, ACTION_VIEW, ACTION_QUERY ]

# options
OPTION_ALL = 'all'

OPTIONS = {
    ACTION_INSERT : [ OPTION_ALL ],
    ACTION_VIEW   : [ OPTION_ALL ],
    ACTION_QUERY  : [ OPTION_ALL ]
}

# contexts
CTX_MACHINE_NAME  = 'machine_name'
CTX_VIEW          = 'view'
CTX_VIEWGROUP     = 'viewgroup'
CTX_USER_IDENTITY = 'user_identity'
CTX_VO_NAME       = 'vo_name'

# allowed context names per actions
CONTEXTS = {
    ACTION_INSERT : [ CTX_MACHINE_NAME ],
    ACTION_VIEW   : [ CTX_VIEW, CTX_VIEWGROUP ],
    ACTION_QUERY  : [ CTX_MACHINE_NAME, CTX_USER_IDENTITY, CTX_VO_NAME ]
}



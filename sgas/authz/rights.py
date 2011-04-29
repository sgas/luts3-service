"""
Authorization rights.
"""


# actions
ACTION_INSERT           = 'insert' # backwards compat
ACTION_JOB_INSERT       = 'jobinsert'
ACTION_STORAGE_INSERT   = 'storageinsert'
ACTION_VIEW             = 'view'
ACTION_MONITOR          = 'monitor'
ACTION_QUERY            = 'query'

ACTIONS = [ ACTION_INSERT, ACTION_JOB_INSERT, ACTION_STORAGE_INSERT, ACTION_VIEW, ACTION_QUERY, ACTION_MONITOR ]

# options
OPTION_ALL = 'all'

OPTIONS = {
    ACTION_INSERT           : [ OPTION_ALL ],
    ACTION_JOB_INSERT       : [ OPTION_ALL ],
    ACTION_STORAGE_INSERT   : [ OPTION_ALL ],
    ACTION_VIEW             : [ OPTION_ALL ],
    ACTION_MONITOR          : [ ],
    ACTION_QUERY            : [ OPTION_ALL ]
}

# contexts
CTX_MACHINE_NAME    = 'machine_name'
CTX_STORAGE_SYSTEM  = 'storage_system'
CTX_VIEW            = 'view'
CTX_VIEWGROUP       = 'viewgroup'
CTX_USER_IDENTITY   = 'user_identity'
CTX_VO_NAME         = 'vo_name'

# allowed context names per actions
CONTEXTS = {
    ACTION_JOB_INSERT       : [ CTX_MACHINE_NAME ],
    ACTION_STORAGE_INSERT   : [ CTX_STORAGE_SYSTEM ],
    ACTION_VIEW             : [ CTX_VIEW, CTX_VIEWGROUP ],
    ACTION_MONITOR          : [ ],
    ACTION_QUERY            : [ CTX_MACHINE_NAME, CTX_USER_IDENTITY, CTX_VO_NAME ]
}



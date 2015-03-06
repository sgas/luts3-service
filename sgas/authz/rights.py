"""
Authorization rights.
"""

OPTION_ALL = 'all'

class Rights():    
    actions  = []
    options  = {}
    contexts = {}
    callback = {}
    
    def addActions(self,action,parseCallback=None):
        self.actions.append(action)
        self.callback[action] = parseCallback


    def addOptions(self,action,options):
        self.options[action] = options
        
        
    def addContexts(self,action,contexts):
        self.contexts[action] = contexts


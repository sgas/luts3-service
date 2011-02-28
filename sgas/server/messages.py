"""
Functionality for storing messages produced during service configuration,
and emitting them once logging functionality is up.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
Copyright: Nordic Data Grid Facility (2011)
"""


from twisted.python import log
from twisted.application import service



class Messages(service.Service):


    def __init__(self):
        self.started = False
        self.messages = [] # [ (message, system) ]


    def startService(self):
        service.Service.startService(self)
        self.started = True
        while self.messages:
            message, system = self.messages.pop(0)
            log.msg(message, system=system)


    def stopService(self):
        service.Service.stopService(self)


    def msg(self, message, system=None):
        if self.started:
            log.msg(message, system=system)
        else:
            self.messages.append((message, system))


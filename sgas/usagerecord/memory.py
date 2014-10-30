"""
SgasMemory class. Representation of the sgas_memory complex type in the database
"""

from psycopg2.extensions import adapt, register_adapter, AsIs

from twisted.python import log

class SgasMemory(object):
    units = {
         "b"    : 1/8,         
         "B"    : 1,
         "KB"   : 1024,
         "MB"   : 1024*1024,
         "GB"   : 1024*1024*1024,
         "TB"   : 1024*1024*1024*1024,
         "PB"   : 1024*1024*1024*1024*1024,
         "EB"   : 1024*1024*1024*1024*1024*1024,
         "Kb"   : 1024/8,
         "Mb"   : 1024*1024/8,
         "Gb"   : 1024*1024*1024/8,
         "Tb"   : 1024*1024*1024*1024/8,
         "Pb"   : 1024*1024*1024*1024*1024/8,
         "Eb"   : 1024*1024*1024*1024*1024*1024/8
    }
    def __init__(self, amount, unit, metric, type):
        self.amount = amount
        self.unit   = self.parseUnit(unit)
        if metric is None:
            metric = "total"
        if metric in ["total","average","min","max"]:
            self.metric = metric
        self.type   = ""
        if type in ["shared","physical","dedicated"]:
            self.type = type
            
    def __conform__(self,obj):        
        return self
    
    def getquoted(self):
        res = "'(%s,%s,%s)'::sgas_memory" % (adapt(self.amount * self.unit), self.metric, self.type)
        return res
               
    def parseUnit(self,unit):
        if unit in self.units:
            return self.units[unit]            
        return 1

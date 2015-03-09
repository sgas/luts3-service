"""
Load class
"""

from sgas.server import config
from twisted.python import log

import re


def loadClassType(cfg, log, type):
    loaded = []
    for section in filter(lambda x:re.search(r'^plugin:.+$',x),cfg.sections()):
        plugin = re.search(r'^plugin:(.+)$',section).group(1)
       
        if not config.PLUGIN_TYPE in cfg.options(section):
            log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_TYPE, section), system='sgas.Setup')
            continue

        if cfg.get(section,config.PLUGIN_TYPE) != type:
            continue
       
        # loadClass
        pluginClass = loadClass(cfg,log,plugin)
        if not pluginClass:
            continue        

        loaded += [pluginClass]
            
    return loaded
            

def loadClass(cfg, log, plugin):
    section = "plugin:%s" % plugin
    if not section in cfg.sections(): 
        log.msg("Plugin: %s can't be loaded :-( section x %s is missing" % (plugin, section), system='sgas.Setup')
        return None
    if not config.PLUGIN_CLASS in cfg.options(section):
        log.msg("Plugin: %s can't be loaded :-( option z %s is missing in %s" % (plugin, config.PLUGIN_CLASS, section), system='sgas.Setup')
        return None
           
    log.msg("Loading %s" % cfg.get(section,config.PLUGIN_CLASS), system='sgas.Setup')
    
    ppackage = re.search(r'^(.*)\.([^\.]+)$',cfg.get(section,config.PLUGIN_CLASS)).group(1)
    pclass = re.search(r'^(.*)\.([^\.]+)$',cfg.get(section,config.PLUGIN_CLASS)).group(2)
              
    # import module
    pluginModule = __import__(ppackage,globals(),locals(),[pclass])
    
    # Create class
    pluginClass = getattr(pluginModule,pclass)
        
    return pluginClass
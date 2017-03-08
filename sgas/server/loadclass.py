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
        log.msg("Plugin: %s can't be loaded :-( section %s is missing" % (plugin, section), system='sgas.Setup')
        return None
    if not config.PLUGIN_CLASS in cfg.options(section):
        log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_CLASS, section), system='sgas.Setup')
        return None
    if not config.PLUGIN_PACKAGE in cfg.options(section):
        log.msg("Plugin: %s can't be loaded :-( option %s is missing in %s" % (plugin, config.PLUGIN_PACKAGE, section), system='sgas.Setup')
        return None
           
    m = re.search(r'^(.*)\.([^\.]+)$',cfg.get(section,config.PLUGIN_PACKAGE))
    if not m:
        log.warning("Plugin: %s can't be loaded; the %s definition seems malformed in %s" % (plugin, config.PLUGIN_PACKAGE, section), system='sgas.Setup')
        return None

    ppackage = m.groups()
    pclass = cfg.get(section,config.PLUGIN_CLASS)
              
    # import module
    pluginModule = __import__(ppackage[0],globals(),locals(),[ppackage[1]])
    
    # Create class
    pluginClass = getattr(getattr(pluginModule,ppackage[1]),pclass)
        
    return pluginClass

"""
Host scale factor updater.

This module will connect to the database (once available), and update the
scaling factors, in the database.

Author: Henrik Thostrup Jensen <htj@ndgf.org>
        Erik Edelmann <erik.edelmann@ndgf.org>
Copyright: Nordic Data Grid Facility (2010), NeIC (2018)
"""

from twisted.python import log
from twisted.internet import defer
from twisted.application import service



INSERT_HOST_SCALE_FACTOR   = '''INSERT INTO hostscalefactors (machine_name, scale_factor) VALUES (%s, %s)'''

# scale options
SCALE_BLOCK      = 'hostscaling'


class HostScaleFactorUpdater(service.Service):

    def __init__(self, cfg, db):
        pass
        self.pool_proxy = db.pool_proxy

        # get scale factors
        self.scale_factors = {}

        if not SCALE_BLOCK in cfg.sections():
            return

        self.default_scale_factor_type = None

        for var in cfg.options(SCALE_BLOCK):
            if var == 'default_scale_factor_type':
                self.default_scale_factor_type = cfg.get(SCALE_BLOCK, var)
                continue

            # Assume we are dealing with an old style scale factor
            # definition, where 'var' is a hostname
            try:
                self.scale_factors[var] = cfg.getfloat(SCALE_BLOCK, var)
            except ValueError:
                log.msg('Invalid scale factor value for entry: %s' % var, system='sgas.Setup')



    def startService(self):
#        raise Exception("The hostscalefactors plugin is obsolete. Use sgas-hs-tool instead!")
        service.Service.startService(self)
        return self.updateScaleFactors()


    def stopService(self):
        pass
        service.Service.stopService(self)
        return defer.succeed(None)


    @defer.inlineCallbacks
    def updateScaleFactors(self):
        try:
            yield self.pool_proxy.dbpool.runInteraction(self.issueUpdateStatements)
            log.msg("Host scale factors updated (%i entries)" % len(self.scale_factors), system='sgas.HostScaleFactorUpdate')
        except Exception, e:
            log.msg('Error updating host scale factors. Message: %s' % str(e), system='sgas.HostScaleFactorUpdate')


    def issueUpdateStatements(self, txn):
        # executed in seperate thread, so it is safe to block

        def get_factor_types():
            """
            Return a factor_type_name -> factor_type_id mapping
            """
            txn.execute("select id, factor_type from hostscalefactor_types;")
            factor_types = {}
            for t_id, t_name in txn.fetchall():
                factor_types[t_name] = t_id
            return factor_types

        def get_machine_names():
            """
            Return a machinename -> machinename_id mapping
            """
            txn.execute("select id, machine_name from machinename;")
            machines = {}
            for m_id, m_name in txn.fetchall():
                machines[m_name] = m_id
            return machines


        factor_types = get_factor_types()

        if self.scale_factors:

            log.msg('Warning: Defining host scale factors in the SGAS config file is deprecated. Use sgas-hs-tool instead!')

            noname_id = factor_types.get("", None)

            if not noname_id:
                log.msg("No unnamed type defined, so we'll define one")
                txn.execute("insert into hostscalefactor_types (factor_type) values ('')")
                factor_types = get_factor_types()
                noname_id = factor_types[""]

            #update_args = [ (hostname, scale_value) for hostname, scale_value in self.scale_factors.items() ]
            update_args = []
            machines = get_machine_names()
            for hostname, scale_value in self.scale_factors.items():
                m_id = machines.get(hostname, None)
                if not m_id:
                    log.msg("Machine name '%s' in the host factor table is unknown - adding to the machinename table" % hostname)
                    txn.execute("insert into machinename (machine_name) values (%s)", [hostname])
                    machines = get_machine_names()
                    m_id = machines[hostname]

                update_args.append((str(m_id), str(noname_id), '(,)', scale_value))


            # clear "old" scale factor and replace with new ones from configuration
            # this is most likely the same as the ones that exists in the database,
            # but it is a cheap operation, and logic is simple
            txn.execute('delete from hostscalefactors_data where scalefactor_type_id = %s', [str(noname_id)])
            txn.executemany('insert into hostscalefactors_data (machine_name_id, scalefactor_type_id, validity_period, scale_factor) VALUES (%s, %s, %s, %s)', update_args)

        if self.default_scale_factor_type != None:
            type_id = factor_types.get(self.default_scale_factor_type, None)
            if type_id:
                txn.execute("update hostscalefactor_type_default set id = %s", [type_id])
                log.msg("Default hostscalefactor_type set to '%s'" % self.default_scale_factor_type)
            else:
                raise Exception("Can't set default; No hostscalefactor_type named '%s' has been defined." % self.default_scale_factor_type or "(empty string)")

        elif "" in factor_types:
            # If no default_scale_factor_type has been specified, assume ""
            # if it exist
            txn.execute("update hostscalefactor_type_default set id = %s", [factor_types[""]])
            log.msg("Default hostscalefactor_type not set, assuming ''")

        else:
            log.msg("No default_scale_factor_type set")

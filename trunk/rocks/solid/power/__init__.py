'''
Power controller
'''

import popen2, sys

import rocks.pssh
from rocks.solid import Launcher
from rocks.solid import module_factory

class BasePower(object) :
    def __init__(self, config) :
        self.config = config
        self.launcher = Launcher()

    def on(self, host_list) :
        pass

    def off(self, host_list) :
        pass

    def reset(self, host_list) :
        pass

    def status(self, host_list) :
        pass

class ClusterPower(rocks.pssh.ClusterFork) :
    def __init__(self, argv, config) :
        rocks.pssh.ClusterFork.__init__(self, argv)
        self.config = config
        power = {}

        try :
            p = module_factory('rocks.solid.power.%s' % (self.config.poweron_driver))
            power['on'] = p.Power(config)
        except ImportError :
            raise IOError('No module named %s' % self.config.poweron_driver)

        try :
            p = module_factory('rocks.solid.power.%s' % (self.config.poweroff_driver))
            power['off'] = p.Power(config)
        except ImportError :
            raise IOError('No module named %s' % self.config.poweroff_driver)

        try :
            p = module_factory('rocks.solid.power.%s' % (self.config.powerreset_driver))
            power['reset'] = p.Power(config)
        except ImportError :
            raise IOError('No module named %s' % self.config.powerreset_driver)

        try :
            p = module_factory('rocks.solid.power.%s' % (self.config.powerstatus_driver))
            power['status'] = p.Power(config)
        except ImportError :
            raise IOError('No module named %s' % self.config.powerstatus_driver)

        self.power = power

    def run(self, command=None):

        if self.nodes:
            nodelist = string.split(self.e.decode(self.nodes), " ")
        else:
            self.connect()
            self.execute(self.query)
            nodelist = []
            for host, in self.cursor.fetchall():
                nodelist.append(host)

        args = self.getArgs()
        if not args :
            self.help()
            sys.exit(1)

        if (len(args) > 1) or not args[0] in ['on', 'off', 'reset', 'status'] :
            self.help()
            sys.exit(1)

        eval("self.power['%s'].%s(nodelist)" % (args[0], args[0]))

if __name__ == '__main__' :
    import sys
    from rocks.solid import rocks_hostlist
    from rocks.solid import config_read
    from rocks.solid.power.ipmi import IPMIPower
    from rocks.solid.power.sw import SWPower

    config = config_read('./rocks-solid.conf')
    #swpower = SWPower(config)
    #swpower.off(rocks_hostlist())
    #ipmi_power = IPMIPower(config)
    #ipmi_power.status(rocks_hostlist())
    #cluster_power = ClusterPower(sys.argv[1:], config)
    #cluster_power.run()

'''
Wake-on-LAN Power Controller
'''
import os, socket, popen2
from rocks_solid import Launcher, rocks_etherlist
from rocks_solid.power import BasePower

class WOLPower(BasePower) :
    '''
    Power on using WOL
    '''
    def __init__(self, config) :
        BasePower.__init__(self, config)
        self.ether_wake = self.config.wol_etherwake
        self.mac_dict = rocks_etherlist(self.config)

    def on(self, host_list, **kwargs) :
        self.launcher.launch(host_list, self.wol, delay = 0.5, **kwargs)

    def wol(self, host) :
        if not self.mac_dict.has_key(host) :
            # Older ROCKS use FQDN
            host = socket.getfqdn(host)
        if not self.mac_dict.has_key(host) :
            # Not found?
            return None, None
        cmd = '%s %s && echo send WOL to %s' % (self.ether_wake, self.mac_dict[host], host)
        ether_wake = popen2.popen4(cmd)
        output = ether_wake[0].read()
        error = ''
        ether_wake[0].close()
        ether_wake[1].close()
        return output, error

Power = WOLPower

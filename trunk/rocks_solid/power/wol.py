'''
Wake-on-LAN Power Controller
'''
import os, socket, popen2
from rocks_solid import Launcher
from rocks_solid.power import BasePower

class WOLPower(BasePower) :
    '''
    Power on using WOL
    '''
    def __init__(self, config) :
        BasePower.__init__(self, config)
        self.dbreport = '/opt/rocks/bin/dbreport ethers'
        self.ether_wake = '/sbin/ether-wake'
        self.mac_dict = {}
        report = os.popen(self.dbreport, 'r')
        while 1 :
            line = report.readline()
            if not line: break
            try :
                mac, hostname = line.strip().split()
            except :
                continue
            self.mac_dict[hostname] = mac
        report.close()

    def on(self, host_list, **kwargs) :
        self.launcher.launch(host_list, self.wol, delay = 0.5, **kwargs)

    def wol(self, host) :
        host = socket.getfqdn(host)
        cmd = '%s %s && echo send WOL to %s' % (self.ether_wake, self.mac_dict[host], host)
        ether_wake = popen2.popen4(cmd)
        output = ether_wake[0].read()
        error = ''
        ether_wake[0].close()
        ether_wake[1].close()
        return output, error

Power = WOLPower

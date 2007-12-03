'''
Wake-on-LAN Power Controller
'''
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

    def on(self, host_list) :
        mac_dict = {}
        report = os.popen(self.dbreport, 'r')
        while 1 :
            line = report.readline()
            if not line: break
            mac, hostname = line.strip().split()
            mac_dict[hostname] = mac
        report.close()
        for host in hostlist :
            os.system('%s %s' % (self.ether_wake, mac_dict[host]))

Power = WOLPower

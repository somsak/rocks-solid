'''
IPMI Power Controller
'''
from rocks.solid import Launcher
from rocks.solid.ipmi import IPMI
from rocks.solid.power import BasePower

class IPMIPower(BasePower) :
    '''
    Power on/off using IPMI
    '''
    def __init__(self, config) :
        BasePower.__init__(self, config)
        self.power_on_cmd = ["power", "on"]
        self.power_off_cmd = ["power", "off"]
        self.power_reset_cmd = ["power", "reset"]
        self.power_status_cmd = ["power", "status"]
        self.ipmi = IPMI(config)

    def on(self, host_list) :
        self.ipmi.cmd_all(host_list, self.power_on_cmd, 0.5)
        
    def off(self, host_list) :
        self.ipmi.cmd_all(host_list, self.power_off_cmd)

    def reset(self, host_list) :
        self.ipmi.cmd_all(host_list, self.power_reset_cmd)

    def status(self, host_list) :
        self.ipmi.cmd_all(host_list, self.power_status_cmd)

Power = IPMIPower

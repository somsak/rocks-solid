'''
Power (up/down) control
'''

import popen2

from rocks.solid import Launcher
from rocks.solid.ipmi import IPMI

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

class SWPower(BasePower) :
    '''
    Power on/off using software (ssh)
    This driver basically do not support power on
    '''
    def on(self, host_list) :
        pass

    def ssh_shutdown(self, host) :
        cmdline = 'ssh %s %s "%s"' % (self.config.ssh_arg, host, self.config.ssh_shutdown_cmd)
        cmd = popen2.Popen3(cmdline, capturestderr = True)
        output = cmd.fromchild.read()
        error = cmd.childerr.read()
        cmd.wait()
        return output, error

    def off(self, host_list) :
        self.launcher.launch(host_list, self.ssh_shutdown)

    def ssh_reboot(self, host) :
        cmdline = 'ssh %s %s "%s"' % (self.config.ssh_arg, host, self.config.ssh_reboot_cmd)
        cmd = popen2.Popen3(cmdline, capturestderr = True)
        output = cmd.fromchild.read()
        error = cmd.childerr.read()
        cmd.wait()
        return output, error

    def reset(self, host_list) :
        self.launcher.launch(host_list, self.ssh_reboot)

class IPMIPower(BasePower) :
    '''
    Power on/off using IPMI
    '''
    def __init__(self, config) :
        BasePower.__init__(self, config)
        self.power_on_cmd = "power on"
        self.power_off_cmd = "power off"
        self.power_reset_cmd = "power reset"
        self.ipmi = IPMI(config)

    def on(self, host_list) :
        self.launcher.launch(host_list, self.ipmi.cmd, [self.power_on_cmd])
        
    def off(self, host_list) :
        self.launcher.launch(host_list, self.ipmi.cmd, [self.power_off_cmd])

    def reset(self, host_list) :
        self.launcher.launch(host_list, self.ipmi.cmd, [self.power_reset_cmd])

if __name__ == '__main__' :
    import sys
    from rocks.solid import rocks_hostlist
    from rocks.solid import config_read
    config = config_read('./rocks-solid.conf')
    swpower = SWPower(config)
    swpower.off(rocks_hostlist())

'''
Software Power
'''

from rocks_solid.power import BasePower
import popen2

class SWPower(BasePower) :
    '''
    Power on/off using software (ssh)
    This driver basically do not support power on
    '''
    def ssh_shutdown(self, host) :
        cmdline = 'ssh %s %s "%s" && echo -n shutdown %s' % (self.config.ssh_arg, host, self.config.ssh_shutdown_cmd, host)
        cmd = popen2.Popen3(cmdline, capturestderr = True)
        output = cmd.fromchild.read()
        error = cmd.childerr.read()
        cmd.wait()
        return output, error

    def off(self, host_list) :
        self.launcher.launch(host_list, self.ssh_shutdown)

    def ssh_reboot(self, host) :
        cmdline = 'ssh %s %s "%s" && echo -n reboot %s' % (self.config.ssh_arg, host, self.config.ssh_reboot_cmd, host)
        cmd = popen2.Popen3(cmdline, capturestderr = True)
        output = cmd.fromchild.read()
        error = cmd.childerr.read()
        cmd.wait()
        return output, error

    def reset(self, host_list) :
        self.launcher.launch(host_list, self.ssh_reboot)

Power = SWPower

if __name__ == '__main__' :
    from rocks_solid import config_read

    config = config_read()
    power = Power(config)
    power.off(['compute-0-0.local'])

'''
Software Power
'''

from rocks.solid.power import BasePower
import popen2

class SWPower(BasePower) :
    '''
    Power on/off using software (ssh)
    This driver basically do not support power on
    '''
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

Power = SWPower

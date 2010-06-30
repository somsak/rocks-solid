'''
IBM Blade Center Power Controller
'''
import subprocess

#from rocks_solid.power import BasePower

#class BladeCenter(BasePower) :
class BladeCenter :
    '''
    Power on/off using BladeCenter
    '''
    def __init__(self, config) :
#        BasePower.__init__(self, config)
        self.port = getattr(config, 'blade_center_ssh_port', 0)
        self.key = getattr(config, 'blade_center_ssh_key', '')
        self.ssh_args = getattr(config, 'blade_center_ssh_args', '')
        self.user = getattr(config, 'blade_center_ssh_user', '')

        # reading the configuration for Blade center
        self.amm_hostnames = {}
        self.targets = {}
        for option in dir(config) :
            if option.startswith('blademm') :
                names = option.split('_')
                section = '_'.join(names[:-1])
                c = names[-1]
                if c == 'hostname' :
                    # AMM hostname or IP address
                    self.amm_hostnames[section] = getattr(config, option)
                elif c.startswith('blade') :
                    # blade position
                    # XXX: The last config will always overwrite
                    host = getattr(config, option)
                    self.targets[host] = {'section': section, 'target' : c}
        for key, value in self.targets.iteritems() :
            if self.amm_hostnames.has_key(value['section']) :
                value['mm'] = self.amm_hostnames[value['section']]
        del self.amm_hostnames

    def lookup_blademm(self, host) :
        return self.targets[host]['mm']

    def lookup_target(self, host) :
        return 'system:' + self.targets[host]['target']

    def send_ssh_command(self, host, command) :
        '''Send specific command to target'''
        # ssh -i key -p port <ssh_args> <user>@<host> command -T target
        cmds = ['ssh']
        if self.key :
            cmds = cmds + ['-i', self.key] 
        if self.port :
            cmds = cmds + ['-p', self.port]
        if self.ssh_args :
            cmds = cmds + self.ssh_args.split()
        if self.user :
            user = self.user + '@'
        else :
            user = ''
        blademm = self.lookup_blademm(host)
        target = self.lookup_target(host)

        cmds = cmds + [ '%s%s' % (user, blademm), command + ' -T ' + target]
        ssh_cmd = subprocess.Popen(cmds, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = ''
        while True :
            line = ssh_cmd.stdout.readline()
            if not line:
                break
            if (not line.strip()) or line.startswith('system>') :
                continue
            output = output + line
        ssh_cmd.wait()

#    def send_all_ssh_command(self, host_list, command) :
#        for host in host_list :
#            print self.send_ssh_command(host, command)

    def on(self, host_list, **kwargs) :
        self.launcher.launch(host_list, self.send_ssh_command, more_arg = 'power -on')
        
    def off(self, host_list, **kwargs) :
        self.launcher.launch(host_list, self.send_ssh_command, more_arg = 'power -off')

    def reset(self, host_list, **kwargs) :
        self.launcher.launch(host_list, self.send_ssh_command, more_arg = 'power -cycle')

    def status(self, host_list, **kwargs) :
        self.launcher.launch(host_list, self.send_ssh_command, more_arg = 'power -state')

Power = BladeCenter

if __name__ == '__main__' :
    class TestConfig :
        blade_center_ssh_port = '2200'
        blade_center_ssh_key = '/root/.ssh/id_rsa_power'
        blade_center_ssh_user = 'POWER'
        blade_center_ssh_arg = '-o BatchMode = yes'
        blademm1_hostname = '203.151.20.87'
        def __init__(self) :
            setattr(self, 'blademm1_blade[1]', 'data1n')
            setattr(self, 'blademm1_blade[2]', 'data2n')
            setattr(self, 'blademm1_blade[3]', 'app1')
            setattr(self, 'blademm1_blade[4]', 'app2')
            setattr(self, 'blademm1_blade[5]', 'app3')
            setattr(self, 'blademm1_blade[6]', 'app4')
    config = TestConfig()

    blade_center = BladeCenter(config)

    print blade_center.targets

    blade_center.status(['app1', 'app2'])

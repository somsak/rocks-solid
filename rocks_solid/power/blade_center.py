'''
IBM Blade Center Power Controller
'''
import subprocess

#from rocks_solid.power import BasePower

#class IBMBladeCenter(BasePower) :
class IBMBladeCenter :
    '''
    Power on/off using IBMBladeCenter
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
        return self.targets[host]['target']

    def send_ssh_command(self, host, command) :
        '''Send specific command to target'''
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
#        cmds = cmds + [
#        subprocess.Popen

    def on(self, host_list, **kwargs) :
        pass
        
    def off(self, host_list, **kwargs) :
        pass

    def reset(self, host_list, **kwargs) :
        pass

    def status(self, host_list, **kwargs) :
        pass

Power = IBMBladeCenter

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

    blade_center = IBMBladeCenter(config)

    print blade_center.targets

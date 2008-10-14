#!/opt/rocks/bin/python
'''
IPMI Launcher
'''
import re, os, sys, string

from rocks_solid import Launcher
import rocks.pssh

class ClusterIPMI(rocks.pssh.ClusterFork) :
    def __init__(self, argv, config) :
        rocks.pssh.ClusterFork.__init__(self, argv)
        self.ipmi = IPMI(config)
        self.usage_name = 'Cluster IPMI'
        self.usage_version = '1.0'

    def usageTail(self) :
        return ' IPMI command'

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
            sys.exit(0)
        self.ipmi.cmd_all(nodelist, self.getArgs())

class IPMI(object) :
    def __init__(self, config) :
        '''
        Initialize IPMI helper

        @type config_file string
        @param config_file configuration file
        '''
        self.config = config

        os.environ['IPMI_PASSWORD'] = self.config.ipmi_passwd
        self.ipmi_arg = '-I ' + self.config.ipmi_intf + ' -E -H %s -U ' + self.config.ipmi_user + ' '

        # host pattern
        if self.config.ipmi_host_pattern.startswith('s/') :
            # substitute pattern
            entry = self.config.ipmi_host_pattern.split('/')
            pattern = entry[1]
            self.repl = entry[2]
            self.regexp = re.compile(pattern)
            self.gen_host = self._host_substitute
        else :
            # execution pattern
            self.gen_host = self._host_command
        self.launcher = Launcher(ignore=config.power_ignore_host)

    def _host_substitute(self, host) :
        '''
        Substitute hostname with specified pattern
        '''
        return self.regexp.sub(self.repl, host)
       
    def _host_command(self, host) :
        '''
        Substitute hostname with command line output
        '''
        cmd = os.popen(self.config.ipmi_host_pattern % host, 'r')
        retval = cmd.read().strip()
        cmd.close()
        return retval

    def gen_hostlist(self, host_list) :
        '''
        Generate IPMI hostlist from host_list, base on configuration

        @type host_list list of string
        @param host_list list of host name
        @rtype list of string
        @return list of IPMI hostname
        '''
        retval = []
        for host in host_list :
            retval.append(self.gen_host(host))
        return retval

#    def iterate(self, host_list) :
#        for host in host_list :
#            yield self.gen_host(host)

    def cmd_all(self, host_list, args, delay = 0, **kwargs) :
        '''
        Issue IPMI command to remote host

        @type host_list list of string
        @param host_list list of host to issue command to
        @type args list of string
        @param args IPMI command
        '''
        new_args = '"'
        new_args = new_args + '" "'.join(args)
        new_args = new_args + '"'
        self.launcher.launch(host_list, self.cmd, [new_args], delay, **kwargs)
    
    def cmd(self, host, args) :
        '''
        Issue IPMI command to a remote host

        @type host string
        @param host host to issue command to
        @type args list of string
        @param args IPMI command
        '''
        real_host = self.gen_host(host)
        exit_stat = os.system('ping -c1 -w1 %s > /dev/null 2>&1' % real_host)
        if os.WIFEXITED(exit_stat) and os.WEXITSTATUS(exit_stat) == 0 :
            cmdline = 'ipmitool ' + self.ipmi_arg % real_host + args
            cmd = os.popen(cmdline, 'r')
            output = cmd.read()
            error = ''
            cmd.close()
        else :
            output = ''
            error = 'down'
        return output, error

if __name__ == '__main__' :
    from rocks_solid import rocks_hostlist
    from rocks_solid import config_read

    ipmi = IPMI(config_read('./rocks-solid.conf'))
    #print ipmi.gen_hostlist(rocks_hostlist())
    #for host in ipmi.iterate(rocks_hostlist()) :
    #    print host
    #ipmi.cmd(rocks_hostlist(), ['power', 'status'])
    ipmi.cmd_all(rocks_hostlist(), ['power', 'status'])

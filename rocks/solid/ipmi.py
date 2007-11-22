#!/opt/rocks/bin/python
'''
IPMI Launcher
'''
import re, os, sys, string

from rocks.solid import config_read
import rocks.pssh

class ClusterIPMI(rocks.pssh.ClusterFork) :
    def __init__(self, argv, config_file) :
        rocks.pssh.ClusterFork.__init__(self, argv)
        self.ipmi = IPMI(config_file)

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

        self.ipmi.cmd(nodelist, self.getArgs())

class IPMI(object) :
    def __init__(self, config_file = None) :
        '''
        Initialize IPMI helper

        @type config_file string
        @param config_file configuration file
        '''
        if config_file :
            self.config = config_read(config_file)
        else :
            self.config = config_read()

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

    def iterate(self, host_list) :
        for host in host_list :
            yield self.gen_host(host)

    def cmd(self, host_list, args) :
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
        for host in self.iterate(host_list) :
            if os.system('ping -c1 -w1 %s > /dev/null 2>&1' % host) == 0 :
                cmdline = 'ipmitool ' + self.ipmi_arg % host + new_args
#                print cmdline
                cmd = os.popen(cmdline, 'r')
                while 1 :
                    line = cmd.readline()
                    if not line :
                        break
                    line = host.split('.')[0][:20] + ':\t' + line
                    sys.stdout.write(line)
                cmd.close()
            else :
                sys.stdout.write('%s:\tdown\n' % host.split('.')[0][:20])

if __name__ == '__main__' :
    from rocks.solid import rocks_hostlist

    #ipmi = IPMI('./rocks-solid.conf')
    #print ipmi.gen_hostlist(rocks_hostlist())
    #for host in ipmi.iterate(rocks_hostlist()) :
    #    print host
    #ipmi.cmd(rocks_hostlist(), ['power', 'status'])

#
# Handle IPMI command
#
import os
import rocks.commands

from rocks.commands.run.host import RocksRemoteCollator

import lekatnet.config as config
import lekatnet.remote as remote

from rocks_solid import config_read
import rocks_solid.ipmi

rs_config = config_read()

rs_config.power_ignore_host = []
    
class command(rocks.commands.HostArgumentProcessor,
    rocks.commands.run.command):

    MustBeRoot = 1

    
class Command(command):
    """
    Run IPMI command for each specified host.

    <arg optional='1' type='string' name='host' repeat='1'>
    Zero, one or more host names. If no host names are supplied, the command
    is run on all known hosts.
    </arg>

    <arg type='string' name='command'>
    The IPMI command to run on the list of hosts.
    </arg>

    <arg type='boolean' name='managed'>
    Run the command only on 'managed' hosts, that is, hosts that generally
    have an ssh login. Default is 'yes'.
    </arg>

    <param type='string' name='command'>
    Can be used in place of the 'command' argument.
    </param>

    <example cmd='run host compute-0-0 command="power status"'>
    Run the IPMI command 'power status' on compute-0-0.
    </example>

    <example cmd='run host compute "power on"'>
    Run the IPMI command 'power on' on all compute nodes.
    </example>
    """

    def run(self, params, args):
        ipmi = rocks_solid.ipmi.IPMI(rs_config)

        (args, command) = self.fillPositionalArgs(('command', ))

        if not command:
            self.abort('must supply a command')
            
        (managed,) = self.fillParams([('managed', 'y')])

        managed_only = self.str2bool(managed)

        hosts = self.getHostnames(args, managed_only)
        
        conf = config.ConfigBase()
        f = open('/etc/tentakel.conf')
        conf.load(f)
        f.close()

        #print '** command ** = ', command
        ipmi.cmd_all(hosts, command)

        #dests = RocksRemoteCollator(self)
        #dests.format = '%o\\n'

        #params = conf.getGroupParams('default')
        # force method
        #params['method'] = 'localcmd'

        #print hosts

        #for host in hosts:
        #    dests.add(remote.remoteCommandFactory(host, params))

        #dests.execAll('echo ipmitool -I lan -H %(host)s -U admin -E ' + command)

        #self.beginOutput()
        #dests.displayAll()
        #self.endOutput(padChar='')
        

RollName = "base"

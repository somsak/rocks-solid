#
# Run local command, supplying tentakel host as argument
#

from lekatnet.remote import registerRemoteCommandPlugin
from lekatnet.remote import RemoteCommand
import time
import commands
import socket
import os
import re
import pexpect
import subprocess

# This plug-in is inspired by 'rocks-ssh' command

class LocalCommand(RemoteCommand):
    "Local command execution class"

    def __init__(self, destination, params):
        # ironically LocalCommand have to inherit RemoteCommand
        RemoteCommand.__init__(self, destination, params)

    def _rexec(self, command):

        t1 = time.time()

        devnull = open('/dev/null', 'r+')
        retval = subprocess.call(['ping', '-c', '1', '-w', '2', self.destination], stdin = devnull, stdout = devnull, stderr = devnull)
        devnull.close()
        if retval != 0 :
            return (-1, 'down')

        # SSH to the machine, this is the same code from the
        # stock SSHRemoteCommand class.

        real_cmd = command % {'host':self.destination}

        try:
            p = pexpect.spawn(real_cmd)
            p.expect(pexpect.EOF)
            output = p.before
            status = 0
        except pexpect.TIMEOUT, e:
            output = "Command timed out on host %s" % \
                self.destination
            status = 1

        p.close()

        self.duration = time.time() - t1
        return (status >> 8, output)


registerRemoteCommandPlugin('localcmd', LocalCommand)


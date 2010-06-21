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

		# The follow test was suggested by Sebastian Stark and
		# replaces our ping test of the machines.

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.settimeout(2)
		try:
			sock.connect((self.destination, 22))
		except socket.error:
			self.duration = time.time() - t1
			return (-1, 'down')

		# SSH to the machine, this is the same code from the
		# stock SSHRemoteCommand class.

		s = '%s %s@%s "%s"' % (self.sshpath, self.user,
			self.destination, command)
		try:
			p = pexpect.spawn(s)
			p.expect(pexpect.EOF)
			output = p.before
			status = 0
		except pexpect.TIMEOUT, e:
			output = "Remote command timed out on host %s" % \
				self.destination
			status = 1

		p.close()

		self.duration = time.time() - t1
		return (status >> 8, output)


registerRemoteCommandPlugin('rocks', RocksSSHRemoteCommand)


'''
IPMI Launcher
'''
import re

from rocks.solid import config_read
import rocks.pssh

class ClusterIPMI(rocks.pssh.ClusterFork) :
    def __init__(self, argv) :
        rocks.pssh.ClusterFork.__init__(self, argv)
        self.config = config_read()

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

		# If no command was supplied just use whatever was
		# left over on the argument list. 

		if not command:
			command = string.join(self.getArgs())
			if not command:
				self.help()
				sys.exit(0)

		sshflags = ""

        # replace hostname in nodelist with hostname in configuration file
        if self.config.ipmi_host_pattern.startswith('s/') :
            # treat it as substitution pattern

        else :
            # treat it as command
			
		for hostname in nodelist:
			sys.stdout.write("%s: " % hostname)
			sys.stdout.flush()

			if os.system('ping -c1 -w1 %s > /dev/null 2>&1' % \
					(hostname)) == 0:
				print ""
					
				if self.bg:
					sshflags = "-f"

				os.system("ssh %s %s \"%s\"" % \
					(sshflags, hostname, command))
			else:
				print "down"

		print ""

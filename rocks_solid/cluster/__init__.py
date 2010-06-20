'''
Base interface for Cluster Management System
'''

import os, subprocess

class ClusterIntf(object) :
    def list_host(self, **kwargs) :
        '''
        List host in the cluster
        This function should return a list of string (host namerocks_cmd)
        '''
        pass

    def list_ethers(self, **kwargs) :
        '''
        Get mapping between host and ethernet address (MAC Address0

        This function should return a dictionary object mapping hostname to MAC
        The hostname should be the same as list_host
        '''
        pass

class Rocks(ClusterIntf) :
    def __init__(self) :
        self.rocks_cmd = ''
        self.dbreport_cmd = ''

        if os.access('/opt/rocks/bin/rocks', os.X_OK) :
            self.rocks_cmd = '/opt/rocks/bin/rocks'
        elif os.access('/opt/rocks/bin/dbreport', os.X_OK) :
            self.dbreport_cmd = '/opt/rocks/bin/dbreport'

    def list_host(self, **kwargs) :
        # sanity check
        if not self.rocks_cmd and not self.dbreport_cmd :
            return []

        host_list = []
        if self.rocks_cmd :
            cmd = subprocess.Popen([self.rocks_cmd, "list", "host"], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        elif self.dbreport_cmd :
            cmd = subprocess.Popen([self.dbreport_cmd, "machines"], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

        first_line = True
        while True :
            line = cmd.stdout.readline()
            if not line:
                break
            if self.rocks_cmd and first_line :
                first_line = False
                continue
            if self.rocks_cmd :
                fields = line.strip().split()
                if fields and len(fields) >= 1 :
                    # stripped out trailing ':'
                    host_list.append(fields[0][:-1])
            elif self.dbreport_cmd :
                host_list.append(line.strip())
        cmd.stdout.close()
        retcode = cmd.wait()

        return host_list
        
    def list_ethers(self, **kwargs) :
        pass

if __name__ == '__main__' :
    rocks = Rocks()
    print rocks.list_host()

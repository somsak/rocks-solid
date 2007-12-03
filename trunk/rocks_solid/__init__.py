'''
Rocks-solid main function
'''

import os, pwd, popen2, string, sys, time, re, types
from StringIO import StringIO
from ConfigParser import ConfigParser

known_system_users = ['fluent', 'accelrys', 'maya', 'autodesk', 'alias']

def module_factory(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def term_userps(user_list = []) :
    ps = os.popen('ps h -eo pid,user', 'r')
    line = ps.readline()
    pid_list = []
    while line :
        pid,user = line.split()
        if user_list and user not in user_list :
            continue
        id = pwd.getpwnam(user)[2]
        if id >= 500 and user not in known_system_users :
            pid_list.append(pid)
        line = ps.readline()
    ps.close()
    if pid_list :
        os.system('kill -s KILL ' + ' '.join(pid_list))

def term_sge_zombie(user_list = []) :
    ps = os.popen('ps h -eo pid,ppid,user', 'r')
    line = ps.readline()
    pid_list = []
    all_pid_list = []
    while line :
        pid,ppid,user = line.split()
        ppid = int(ppid)
        pid = int(pid)
        all_pid_list.append((pid, ppid, user))
        if user_list and user not in user_list :
            continue
        id = pwd.getpwnam(user)[2]
        if (id >= 500) and (ppid == 1) :
            pid_list.append(pid)
        line = ps.readline()
    ps.close()
    # second pass
    if pid_list :
        pid2_list = []
        for psinfo in all_pid_list :
            if psinfo[1] in pid_list :
                pid2_list.append(int(psinfo[0]))
        pid_list = pid_list + pid2_list
        os.system('kill -s KILL ' + ' '.join(map(str, pid_list)))

def cleanipcs(ipc_list = []) :
    '''
    Clean orphaned IPC
    '''
    if not ipc_list :
        ipc_list = ['sem', 'shm']
    for ipc in ipc_list :
        if ipc == 'sem' :
            ipc_opt = '-s'
        elif ipc == 'shm' :
            ipc_opt = '-m'
        ipcs = os.popen('ipcs -p ' + ipc_opt)
        id_list = []
        while 1 :
            line = ipcs.readline()
            if not line :
                break
            try :
                int(line[0])
            except ValueError :
                continue
            ipc_data = line.split()
            # get process information
            pids = ipc_data[2:3]
            for pid in pids :
                if not os.path.exists('/proc/%s' % pid) :
                    id_list.append(ipc_data[0])
        ipcs.close()
        if id_list :
            opt_str = ' ' + ipc_opt + ' '
            options = opt_str + opt_str.join(id_list)
            os.system('ipcrm ' + options)

def rocks_hostlist() :
    if os.access('/opt/rocks/bin/rocks', os.X_OK) :
        cmd = popen2.Popen4('/opt/rocks/bin/rocks list host | grep -v Frontend ')
        retval = []
        while 1 :
            line = cmd.fromchild.readline()
            if not line :
                break
            if line.startswith('HOST') :
                continue
            line = line.strip()
            if not line :
                continue
            retval.append(line.split()[0].replace(':', ''))
        cmd.wait()
    else :
        cmd = popen2.Popen4('dbreport machines')
        retval = cmd.fromchild.readlines()
        retval = map(string.strip, retval)
        cmd.wait()
    return retval

class Config :
    poweron_driver = 'ipmi'
    poweroff_driver = 'sw'
    powerreset_driver = 'sw'
    powerstatus_driver = 'ipmi'
    scheduler = 'sge'
    power_min_spare = 5
    power_ignore_host = []
    ssh_shutdown_cmd = '/sbin/poweroff'
    ssh_reboot_cmd = '/sbin/reboot'
    ssh_arg = ''
    ipmi_host_pattern = 's/compute/compute-ilo/g'
    ipmi_user = 'admin'
    ipmi_passwd = ''
    ipmi_intf = 'lanplus'
    db_uri = 'sqlite:///var/spool/rocks/power_control.db'
    power_ignore_host = []
    default_queue = ''

def config_read(file = os.sep + os.path.join('etc', 'rocks-solid.conf')) :
    '''
    Read configuration file and return configuration object

    @rtype configuration object
    @return configuration object
    '''
    config_parser = ConfigParser()
    if os.environ.has_key('ROCKS_SOLID_CONF') :
        file = os.environ['ROCKS_SOLID_CONF']
    config_parser.read(file)

    stat_data = os.stat(file)
    if stat_data.st_mode & 0077 :
        raise IOError('wrong permission of %s, must not be world-readable' % file)

    config = Config()

    for sect in config_parser.sections() :
            for opt in config_parser.options(sect) :
                if sect == 'main' :
                    setattr(config, opt, config_parser.get('main', opt))
                else :
                    setattr(config, sect + '_' + opt, config_parser.get(sect, opt))
    if config.power_ignore_host :
        config.power_ignore_host = config.power_ignore_host.split(',')
        for i in range(len(config.power_ignore_host)) :
            config.power_ignore_host[i] = re.compile(config.power_ignore_host[i])

    if type(config.power_min_spare) != types.IntType :
        config.power_min_spare = int(config.power_min_spare)

    del config_parser

    return config

class Launcher(object) :
    def __init__(self, **kw) :
        self.ignore = kw.get('ignore', [])

    def launch(self, host_list, func, more_arg = None, delay = 0) :
        for host in host_list :
            skip = 0
            for pattern in self.ignore :
                if pattern.match(host) :
                    skip = 1
                    break
            if skip :
                continue
            if delay > 0:
                time.sleep(delay)
            #elif delay == -1
            #   do background
            if more_arg :
                output, error = func(host, *more_arg)
            else :
                output, error = func(host)
            for o in output, error :
                soutput = StringIO(o)
                while 1 :
                    line = soutput.readline()
                    if not line : break
                    sys.stdout.write(host.split('.')[0][:20] + ':\t' + line)
                soutput.close()

if __name__  == '__main__' :
    c = config_read('./rocks-solid.conf')
    for attr in dir(c) :
        print attr, getattr(c, attr)
    l = Launcher()
    def test_func(host, ab, cd) :
        print host, ab, cd
    l.launch(['compute-0-0', 'compute-0-1', 'compute-0-2'], test_func, ['test1', 'test2'])

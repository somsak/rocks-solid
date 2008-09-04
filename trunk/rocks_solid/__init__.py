'''
Rocks-solid main function
'''

import os, pwd, popen2, string, sys, time, re, types, shutil
from StringIO import StringIO
from ConfigParser import ConfigParser
from threading import Thread, Condition

known_system_users = ['fluent', 'accelrys', 'maya', 'autodesk', 'alias']

def module_factory(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def term_userps(user_list = [], exclude_list = [], **kwargs) :
    ps = os.popen('ps h -eo pid,user', 'r')
    pid_list = []
    debug = kwargs.get('debug', False)
    while 1 :
        line = ps.readline()
        if not line :
            break
        pid,user = line.split()
        if exclude_list and user in exclude_list :
            if debug : print >> sys.stderr, 'exclude user ', user
            continue
        if user_list and user not in user_list :
            if debug : print >> sys.stderr, 'user %s not in user list' % (user)
            continue
        id = pwd.getpwnam(user)[2]
        if id >= 500 and user not in known_system_users :
            pid_list.append(pid)
        line = ps.readline()
    ps.close()
    if pid_list :
        if debug : 
            print >> sys.stderr, 'terminate processes %s' % ' '.join(pid_list)
        else :
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
    power_loadavg = 0.2
    power_db = 'sqlite:////var/tmp/host_activity.sqlite'

def config_read(file = os.sep + os.path.join('etc', 'rocks-solid.conf')) :
    '''
    Read configuration file and return configuration object

    @rtype configuration object
    @return configuration object
    '''
    config_parser = ConfigParser()
    if os.environ.has_key('ROCKS_SOLID_CONF') :
        file = os.environ['ROCKS_SOLID_CONF']
    if not os.access(file, os.R_OK) :
        raise IOError('can not access config file %s' % file)
    config_parser.read(file)

    stat_data = os.stat(file)
# remove since WOL and SW don't need this kind of strict permission
#    if stat_data.st_mode & 0077 :
#        raise IOError('wrong permission of %s, must not be world-readable' % file)

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

    if type(config.temp_thereshold) != types.IntType :
        config.temp_thereshold = int(config.temp_thereshold)

    if type(config.power_loadavg) != types.FloatType :
        config.power_loadavg = float(config.power_loadavg)

    del config_parser

    return config

def check_ignore(host_name, pattern_list, verbose = False) :
    '''
    Check for each host name against pattern list

    @type host_name string
    @param host_name host name to check
    @type pattern_list list of compiled RE
    @param pattern_list list of pattern to check
    @rtype boolean
    @return True if matched, otherwise false
    '''
    for ent in pattern_list :
        if ent.match(host_name) :
            if verbose :
                print >> sys.stderr, 'pattern %s match %s' % (ent.pattern, host_name)
            return True
    return False

class Launcher(object) :
    def __init__(self, **kw) :
        self.ignore = kw.get('ignore', [])
        self.condition = None
        self.num_thread = 0
        self.count_thread = 0
        self.output = {}
        self.thread_list = {}

    def thread_run(self, launcher, func, args) :
        try :
            output, error = func(*args)
        except :
            import traceback
            print >> sys.stderr, 'Host %s error!' % args[0]
            traceback.print_exc()

        launcher.condition.acquire()
        try :
            launcher.output[args[0]] = (output, error)
        except :
            pass
        launcher.condition.notify()
        launcher.condition.release()

    def launch(self, host_list, func, more_arg = None, delay = 0, num_thread = 10) :
        if delay < 0 :
            self.num_thread = num_thread
            self.condition = Condition()
            self.count_thread = 0
            self.condition.acquire()

        for host in host_list :
            skip = 0
            for pattern in self.ignore :
                if pattern.match(host) :
                    skip = 1
                    break
            if skip :
                continue
            if more_arg :
                args = [host] + more_arg
            else :
                args = [host]

            if delay >= 0:
                time.sleep(delay)
                output, error = func(*args)
                self.print_output(host, output, error)
            elif delay < 0 :
                while self.output :
                    key, value = self.output.popitem()
                    self.print_output(key, value[0], value[1])
                    self.count_thread -= 1
                t = Thread(target = self.thread_run, args = (self, func, args))
                t.start()
                self.count_thread += 1
                if self.count_thread >= self.num_thread :
                    self.condition.wait()
        if delay < 0 :
            while self.count_thread > 0 :
                while self.output :
                    key, value = self.output.popitem()
                    self.print_output(key, value[0], value[1])
                    self.count_thread -= 1
                if self.count_thread > 0 :
                    self.condition.wait()
            self.condition.release()
            self.count_thread = 0
            self.condition = None
                    
    def print_output(self, host, output, error) :
        for o in output, error :
            soutput = StringIO(o)
            while 1 :
                line = soutput.readline().strip()
                if not line : break
                sys.stdout.write(host.split('.')[0][:20] + ':\t' + line + '\n')
            soutput.close()

def _get_pid(path) :
    f = open(os.path.join(path, 'data'), 'r')
    pid = f.readline().strip()
    f.close()
    return int(pid)

def acquire_lock(path) :
    '''
    Acquire a lock

    @arg path path of lock
    @type string
    @return handle of current lock, or None if lock failed
    '''
    data_path = os.path.join(path, 'data')
    while True :
        try :
            os.mkdir(path)
            f = open(data_path, 'w')
            f.write('%d\n' % os.getpid())
            f.close()
            return path
        except Exception, e:
            pid = None
            try :
                pid = _get_pid(path)
            except :
                pass
            if pid and os.path.exists('/proc/%d' % pid) :
                return None
            else :
                shutil.rmtree(path)

def release_lock(handle) :
    '''
    Release a lock

    @arg handle handle returned by acquire_lock
    @type string
    '''
    path = handle
    try :
        pid = _get_pid(path)
        if os.getpid() == pid :
            shutil.rmtree(path)
    except :
        pass

if __name__  == '__main__' :
    import tempfile
    c = config_read('./rocks-solid.conf')
    for attr in dir(c) :
        print attr, getattr(c, attr)
    l = Launcher()
    def test_func(host, ab, cd) :
        print host, ab, cd
    l.launch(['compute-0-0', 'compute-0-1', 'compute-0-2'], test_func, ['test1', 'test2'])
    path = os.path.join(tempfile.gettempdir(), 'test.lock')
    print acquire_lock(path)
    print acquire_lock(path)
    print release_lock(path)
    print release_lock(path)



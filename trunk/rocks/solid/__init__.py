'''
Rocks-solid main function
'''

import os, pwd, popen2, string

known_system_users = ['fluent', 'accelrys', 'maya', 'autodesk', 'alias']

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

def cleanipcs(ipc_list = [], user_list = []) :
    if not ipc_list :
        ipc_list = ['sem', 'shm']
    for ipc in ipc_list :
        if ipc == 'sem' :
            ipc_opt = '-s'
        elif ipc == 'shm' :
            ipc_opt = '-m'
        ipcs = os.popen('ipcs -c ' + ipc_opt)
        id_list = []
        while 1 :
            line = ipcs.readline()
            if not line : break
            try :
                int(line[0])
            except ValueError :
                continue
            ipc_data = line.split()
            uid = pwd.getpwnam(ipc_data[2])[2]
            if uid < 500 or ipc_data[2] in known_system_users :
                continue
            if user_list :
                if ipc_data[2] not in user_list :
                    continue
            id_list.append(ipc_data[0])
        ipcs.close()
        if id_list :
            opt_str = ' ' + ipc_opt + ' '
            options = opt_str + opt_str.join(id_list)
            os.system('ipcrm ' + options)

def all_hosts() :
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

if __name__ == '__main__' :
    print all_hosts()

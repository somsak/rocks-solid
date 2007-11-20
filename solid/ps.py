'''
process control
'''
import os, pwd

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


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


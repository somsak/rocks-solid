'''
ipc functionality

'''

from rocks.solid.ps import known_system_users

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


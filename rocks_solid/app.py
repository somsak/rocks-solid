#!/opt/rocks/bin/python
'''
Application file
'''

def run_cluster_ipmi() :
    import sys
    import rocks_solid.ipmi
    from rocks_solid import config_read

    app = rocks_solid.ipmi.ClusterIPMI(sys.argv, config_read())
    app.parseArgs()
    app.run()

def run_cluster_power() :
    import sys
    import rocks_solid.power
    from rocks_solid import config_read

    app = rocks_solid.power.ClusterPower(sys.argv, config_read())
    app.parseArgs()
    app.run()

def run_node_cleanipcs() :
    import optparse
    from rocks_solid import cleanipcs

    parser = optparse.OptionParser(usage='%prog [sem|shm]')
    options, args = parser.parse_args()
    if len(args) > 0 :
        ipc_list = args[0]
        assert args[0] in ['sem', 'shm']
    else :
        ipc_list = []
    cleanipcs(ipc_list)

def run_node_term_user_ps() :
    import optparse
    from rocks_solid import cleanipcs
    from rocks_solid import term_userps

    parser = optparse.OptionParser(usage='%prog <user1> <user2> ...')
    options, args = parser.parse_args()
    user_list = args
    term_userps(user_list)

def run_node_term_sge_zombie() :
    import optparse
    from rocks_solid import cleanipcs
    from rocks_solid import term_sge_zombie

    parser = optparse.OptionParser(usage='%prog <user1> <user2> ...')
    options, args = parser.parse_args()
    user_list = args
    term_sge_zombie(user_list)

def run_cluster_freehost() :
    import sys, os, optparse
    from rocks_solid import module_factory
    from rocks_solid import config_read

    show_host = True
    show_number = True
    parser = optparse.OptionParser()
    parser.add_option('-n', '--number', dest='number', action="store_true",
        help="Only print number of free host(s)")
    parser.add_option('-H', '--hosts', dest='hosts', action="store_true",
        help="Only print name of free host(s)")
    options, args = parser.parse_args()
    if options.number :
        show_host = False
    if options.hosts :
        show_number = False

    config = config_read()
    try :
        scheduler_mod = module_factory('rocks_solid.scheduler.%s' % config.scheduler)
        scheduler = scheduler_mod.Scheduler()
        online_hosts, offline_hosts = scheduler.hosts()
        # build dictionary of online_hosts
        online_hosts_dict = {}
        for host in online_hosts :
            online_hosts_dict[host.name] = 1
        queues = scheduler.queues()
        for q in queues :
            for host in q.online_hosts :
                if host.slot_used > 0:
                    try :
                        del online_hosts_dict[host.name]
                    except :
                        pass
        num_host = None
        if show_host :
            num_host = 0
            for host in online_hosts_dict.iterkeys() :
                print host
                num_host = num_host + 1
        if show_number :
            if num_host is not None :
                print num_host
            else :
                print len(online_hosts_dict.keys())
            
    except ImportError :
        parser.error('Unknown scheduler setting: %s' % config.scheduler)
        sys.exit(1)

def run_cluster_clean_ps() :
    import sys, os
    from rocks_solid import config_read

    config = config_read()
    try :
        ##
        ## no need for all this anymore. cleanipcs and term-sge can be used safely
        ##
        #scheduler_mod = module_factory('rocks_solid.scheduler.%s' % config.scheduler)
        #scheduler = scheduler_mod.Scheduler()
        #online_hosts, offline_hosts = scheduler.hosts()
        # build dictionary of online_hosts
        #online_hosts_dict = {}
        #for host in online_hosts :
        #    online_hosts_dict[host.name] = 1
        #queues = scheduler.queues()
        #for q in queues :
        #    for host in q.online_hosts :
        #        if host.slot_used > 0:
        #            try :
        #                del online_hosts_dict[host.name]
        #            except :
        #                pass
        # clean SGE zombie first
        # use tentakel because we want to run it only on compute nodes
        os.system('tentakel -g compute node-term-%s-zombie' % config.scheduler)
        #fd, path = tempfile.mkstemp()
        #tmpfile = os.fdopen(fd)
        #for host in online_hosts_dict.iterkeys() :
        #    tmpfile.write(host + '\n')
        #tmpfile.close()
        # use cluster-fork because tentakel don't accept external machine file
        #os.system('cluster-fork --pe-hostfile %s --bg node-term-user-ps' % path)
        #os.system('cluster-fork --pe-hostfile %s --bg node-cleanipcs' % path)
        os.system('tentakel -g compute node-cleanipcs')
        #os.remove(path)
    except ImportError :
        parser.error('Unknown scheduler setting: %s' % config.scheduler)
        sys.exit(1)

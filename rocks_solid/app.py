#!/opt/rocks/bin/python
'''
Application file
'''

powersave_ignore_file = '/var/tmp/powersave_ignore'

def run_cluster_ipmi() :
    import sys
    import rocks_solid.ipmi
    from rocks_solid import config_read

    config = config_read()
    # never ignore hosts
    config.power_ignore_host = []
    app = rocks_solid.ipmi.ClusterIPMI(sys.argv, config)
    app.parseArgs()
    app.run()

def run_cluster_power() :
    import sys
    import rocks_solid.power
    from rocks_solid import config_read

    config = config_read()
    # never ignore hosts
    config.power_ignore_host = []
    app = rocks_solid.power.ClusterPower(sys.argv, config)
    app.parseArgs()
    app.run()

def run_node_cleanipcs() :
    import optparse
    from rocks_solid import cleanipcs

    parser = optparse.OptionParser(usage='%prog [sem|shm]')
    options, args = parser.parse_args()
    if len(args) > 0 :
        ipc_list = [args[0]]
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
            if not q.online_hosts :
                continue
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
        # use tentakel because we want to run it only on compute nodes
        os.system('tentakel -g compute node-term-%s-zombie' % config.scheduler)
        os.system('tentakel -g compute node-cleanipcs')
    except ImportError :
        parser.error('Unknown scheduler setting: %s' % config.scheduler)
        sys.exit(1)

def run_cluster_powersave() :
    import optparse, os
    from rocks_solid import config_read, check_ignore
    from rocks_solid import module_factory
    from rocks_solid.power import ClusterPower
    from rocks_solid.db import DB

    parser = optparse.OptionParser()
    parser.add_option('-d', '--dryrun', dest='dryrun', action="store_true", default=False,
        help="just test, no action taken")
    parser.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
        help="verbose output")

    options, args = parser.parse_args()

    # query queue information
    config = config_read()

    # initializa host activity database
    db = DB(url = config.power_db, verbose = options.verbose)

    # do we need to ignore everything? 
    if os.path.exists(powersave_ignore_file) or \
        os.path.exists('/etc/nologin') :
        sys.exit(1)
    try :
        scheduler_mod = module_factory('rocks_solid.scheduler.%s' % config.scheduler)
        scheduler = scheduler_mod.Scheduler()
        # query queue information
        queues = scheduler.queues()
        # queue_dict hold 'locally free' host
        queue_dict = {}
        # all_free_hosts hold 'globally free' host
        all_free_hosts = {}
        all_offline_hosts = {}
        all_online_hosts = {}
        # look for free host and off line hosts for each queue
        for queue in queues :
            #print '******* %s *******' % queue.name
            queue_dict[queue.name] = ([], queue.offline_hosts)
            if queue.offline_hosts :
                for host in queue.offline_hosts :
                    if not check_ignore(host.name, config.power_ignore_host) :
                        all_offline_hosts[host.name] = host
            if not queue.online_hosts :
                continue
            for host in queue.online_hosts :
#                if host.name == 'compute-4-11.local' :
#                    print host
                all_online_hosts[host.name] = 1
                if (host.slot_used <= 0) and (host.loadavg < config.power_loadavg):
                    queue_dict[queue.name][0].append(host)
                    if not all_free_hosts.has_key(host.name) :
#                        print 'adding %s' % host.name
                        if not check_ignore(host.name, config.power_ignore_host) :
                            all_free_hosts[host.name] = host
                else :
#                    print 'removing %s' % host.name
                    if not check_ignore(host.name, config.power_ignore_host) :
                        all_free_hosts[host.name] = None
        for key in all_free_hosts.keys() :
            if not all_free_hosts[key] :
                del all_free_hosts[key]

        if options.verbose :
            print '******* All on-line and free hosts *******'
            for key in all_free_hosts.iterkeys() :
                print key

        if not options.dryrun :
            db.insert_on_hosts(all_online_hosts.keys())

        if config.default_queue :
            default_queue = config.default_queue
        else :
            # pick the first queue
            default_queue = queues[0].name

#        if options.verbose :
#            for item in queue_dict.iteritems() :
#                print item[0], item[1]

        # query job list information
        job_list = scheduler.list()
        i = 0
        while i < len(job_list) :
            if job_list[i].state == 'running' :
                del job_list[i]
            else :
                if not job_list[i].queue :
                    job_list[i].queue = default_queue
                i = i + 1
            
#        for job in job_list :
#            print job

        # for each queue which has job pending, pick enough host for that queues
        # decrease number of free host base on each job
        poweron_hosts = []
        poweroff_hosts = []
        if options.verbose :
            print '***** all offline hosts *****' 
            print all_offline_hosts
        for job in job_list :
            while job.np > 0 :
                # we deal only with off line hosts here
                # there's no simple way to imitate resource
                # selection of underlying scheduler
                # we'll randomly pick any off line hosts
                # until the job is run
                if all_offline_hosts :
                    # randomly pick hosts from offline hosts
                    name, host = all_offline_hosts.popitem()
                    if options.verbose :
                        print 'select %s, %s' % (name, host)
                        print name, host
                    job.np -= host.slot_total
                    poweron_hosts.append(host.name)
                else :
                    # no hosts left!
                    # break immediately
                    break
            # it is possible that off line hosts is not enough
            # in that case the job would *still* stuck in wait
            # while hosts are being powered on
            # we can't do anything about that
        # only power down if there's no waiting job
        power = ClusterPower(None, config)
        if not job_list :
            # all free hosts that's left, power it down!
            poweroff_hosts = all_free_hosts.keys()
            #print poweroff_hosts
            # vacant nodes - power off nodes - min_spare. 
            if len(poweroff_hosts) > config.power_min_spare :
                poweroff_hosts = poweroff_hosts[config.power_min_spare:]
            else :
                poweroff_hosts = []

#            print len(poweroff_hosts)
#            print poweroff_hosts
#            print poweron_hosts

            # power off ndoes
            if poweroff_hosts :
                power.nodes = poweroff_hosts
                if not options.dryrun :
                    db.update_hosts(poweroff_hosts, 'off')
                    power.run(command=['off'])
                else :
                    print 'power down %s' % poweroff_hosts
            # power on nodes
        if poweron_hosts :
            power.nodes = poweron_hosts
            if not options.dryrun :
                db.update_hosts(poweron_hosts, 'on')
                power.run(command=['on'])
            else :
                print 'power on %s' % poweron_hosts
    except :
        raise

def log_date(file) :
    import time

    f = open(file, 'w')
    f.write(time.asctime() + '\n')
    f.close()

def run_node_envcheck() :
    import optparse
    from rocks_solid import config_read
    from rocks_solid import module_factory

    parser = optparse.OptionParser()
    parser.add_option('-d', '--dryrun', dest='dryrun', action="store_true",
        help="just test, no action taken")
    parser.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
        help="verbose output")

    options, args = parser.parse_args()
    config = config_read()
    if options.verbose :
        setattr(config, 'verbose', True)
    else :
        setattr(config, 'verbose', False)
    try :
        log = config.env_log
    except :
        log = '/var/log/node-envcheck.log'
    checkers_cf = config.env_sensor.split(',')
    checkers = []
    for c in checkers_cf :
        m = module_factory('rocks_solid.env.%s' % c)
        checkers.append( (m.Checker(config), c))
    action_cf = config.env_action
    m = module_factory('rocks_solid.env.%s' % action_cf)
    action = m.Action(config)
    all_retval = []
    for checker in checkers :
        retval = checker[0].check()

        if options.verbose :
            print 'return value of %s = %d' % (checker[1], retval)
        if retval != 0 :
            if config.env_criteria == 'any' :
                if not options.dryrun :
                    log_date(log)
                    action.act()
                break
            else :
                all_retval.append(retval)
    if all_retval :
        if not 0 in all_retval :
            if not options.dryrun :
                log_date(log)
                action.act()

def run_check_ignore_host() :
    import optparse
    from rocks_solid import config_read, check_ignore
    from rocks_solid import module_factory

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', dest='config', 
        help="path to configuration file")
    parser.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
        help="verbose output")

    options, args = parser.parse_args()
    config = config_read()
    for arg in args :
        print 'checking %s = %d' % (arg, check_ignore(arg, config.power_ignore_host, options.verbose))

def run_queue_limit_user_cpu() :
    '''
    Terminate any job exceed the usage of number of CPU core per job
    '''
    import optparse, sys, fnmatch
    from rocks_solid import config_read
    from rocks_solid import module_factory
    
    parser = optparse.OptionParser(usage='%prog <number of CPU>')
    parser.add_option('-c', '--config', dest='config', 
        help="path to configuration file")
    parser.add_option('-v', '--verbose', dest='verbose', action="store_true", default=False,
        help="verbose output")
    parser.add_option('-d', '--dry-run', dest='dry_run', action="store_true", default=False,
        help="dry run (just test, no cancel)")
    parser.add_option('-q', '--queue', dest='queue', default=None,
        help="queue name (default is all queue)")
    parser.add_option('-o', '--owner', dest='owner', default='*',
        help="owner of job (wildcard is possible here. default:*)")
    options, args = parser.parse_args()
    config = config_read()
    if len(args) < 1 :
        parser.error('need at least one argument')
    queue = options.queue
    owner = options.owner
    if options.verbose :
        print >> sys.stderr, 'Queue to check: %s' % (queue or 'all queues')
        print >> sys.stderr, 'User to check: %s' % (owner)
    ncore = int(args[0])
    scheduler_mod = module_factory('rocks_solid.scheduler.%s' % config.scheduler)
    scheduler = scheduler_mod.Scheduler()
    job_list = scheduler.list()
    for job in job_list :
        if ((job.queue == queue) or (queue is None)) and \
            fnmatch.fnmatch(job.owner, owner) and \
            (job.np > ncore) :
            if options.verbose :
                print >> sys.stderr, 'Cancelling job %s, named %s, of user %s' % (job.jid, job.name, job.owner)
            if not options.dry_run :
                scheduler.cancel(job.jid)

if __name__ == '__main__' :
    run_cluster_powersave()

'''
TORQUE/PBS implementation
'''
# vim: ft=python ts=4 sw=4 sta et sts=4 ai:
import os, socket, re, time, copy, types
from datetime import datetime
from StringIO import StringIO
import popen2

import elementtree.ElementTree as ET
try :
    from animagrid.scheduler import BaseScheduler,JobInfo,Host,Queue
except ImportError :
    # For Rocks-solid
    from rocks_solid.scheduler import BaseScheduler,JobInfo,Host,Queue

state_map = {
    'pending': 'waiting',
    'running': 'running',
}

_pbs_state = {
    'Q': 'waiting',
    'R': 'running',
    'B': 'running',
}

def _pbs_get_state(state) :
    if _pbs_state.has_key(state) :
        return _pbs_state[state]
    else :
        return 'error'
qsub_re = re.compile(r'^(?P<jid>[0-9]+)[\[\]]?[.]?(?P<host>.*)$', re.IGNORECASE) # Fixed for PBS
qdel_re = re.compile(r'.*\s+has\s+(deleted\s*job(-array\s+task)?|registered\s+the\s+job(-array\s+task)?)\s+(?P<jid>[0-9.]+).*', re.IGNORECASE)
qstat_qname_re = re.compile(r'^Queue:\s+(?P<qname>.*)$', re.IGNORECASE)
qstat_qattr_re = re.compile(r'^\s+(?P<key>[^=]+)\s+=\s+(?P<value>[^=]+)$', re.IGNORECASE)

class Scheduler(BaseScheduler):
    def __init__(self, conf={}, **kw):
        BaseScheduler.__init__(self, conf, **kw)

        self.name = 'pbs'
        self.dry_run = kw.get('dry_run', False)
        self.logger = kw.get('logger', None)

        # translation table for job script environment
        self.job_env_vars = {
            'job_id': 'PBS_JOBID',
            'job_name': 'PBS_JOBNAME',
            'task_id': 'PBS_ARRAYID',
        }

        # PBS commands
        # reasonable default
        self.pbs_home = kw.get('pbs_home', '/opt/torque')
        self.qstat = 'qstat'
        self.qsub = 'qsub'
        self.qdel = 'qdel'
        self.pbsnodes = 'pbsnodes'
        # exact path
        for cmd in ['qstat', 'qsub', 'qdel', 'pbsnodes'] :
            path = os.path.join(self.pbs_home, 'bin', cmd)
            if os.access(path, os.X_OK) :
                setattr(self, cmd, path)
        self.extend_status = conf.get('extend_status', False)
        self.scheduler = 'pbs'

    def _job_filter(self, filter, jid) :
        '''
        Check whether specified jid is in filter or not

        @type filter list of string
        @param filter list of job identifier (may control task id)
        @type jid string
        @param jid job identifier
        @rtype boolean
        @return true if contains in filter, otherwise false
        '''
        job = jid.split('.', 1)
        if len(job) >= 2 :
            job = job[0]
        for ent in filter :
            if ent == jid :
                return True
            elif ent == job :
                return True
            continue
        return False

    def list(self, filter = None, **kw):
        '''
        List the jobs in system

        @type filter list of string
        @param filter list of job id to get status
        @rtype list of JobInfo
        @return list of JobInfo class
        '''
        job_construct = kw.get('job', JobInfo)
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        cmd = popen2.Popen4(sudo_cmd + '%s -f' % self.qstat)

        def add_job(array_all, array_waiting) :
            if array_all is None  :
                jobs.append(job)
            else :
                for i in range(array_all[0], array_all[1] + 1) :
                    new_task = copy.deepcopy(job)
                    new_task.tid = i
                    new_task.state = 'running'
                    if array_waiting and (i >= array_waiting[0]) and (i <= array_waiting[1]) :
                        new_task.state = 'waiting'
                    jobs.append(new_task)
                    array_waiting = None
                    array_all = None

        jobs = []
        job = job_construct()
        next_line_buffer = None
        array_waiting = None
        array_all = None

        while 1 :
            if not next_line_buffer is None :
                line = next_line_buffer
                next_line_buffer = None
            else :
                line = cmd.fromchild.readline()
            if not line: break
            line_stripped = line.strip()
            if not line_stripped : continue # blank line 
            # check for "continuous" line
            if line.startswith('    ') :
                # attribute line might have other lines after this
                # but we will only look for lines that we're interest
                if line_stripped.startswith('exec_host') :
                    # there might be other line after this
                    while 1 :
                        next_line = cmd.fromchild.readline()
                        if next_line and next_line.startswith('\t') : 
                            line = line.rstrip() + next_line.strip()
                            line_stripped = line.strip()
                            next_line_is_red = True
                        else :
                            next_line_buffer = next_line
                            break
            if line.startswith('Job Id') :
                # add new job to the list, if exist
                if not (job.jid is None) : 
                    add_job(array_all, array_waiting)
                    job = job_construct()
                    job.scheduler = self.scheduler

                jid = line.split(':')[1]
                jid = jid.strip()
                if jid.find('[]') < 0 :
                    job.jid, job.scheduler_host = jid.split('.', 1)
                else :
                    # this is an array job
                    job.jid, job.scheduler_host = jid.split('.', 1)
                    job.jid = job.jid.replace('[]', '')
                    # we handle array job later
                if filter and (not self._job_filter(filter, job.jid)) :
                    # we'll ignore this entry completely
                    job.jid = None

            elif not (job.jid is None) :
                fields = line_stripped.split('=', 1)
                if len(fields) <= 1 :
                    continue
                key, value = fields
                key = key.strip()
                value = value.strip()
                if key == 'Job_Name' :
                    job.name = value
                elif key == 'Job_Owner' :
                    job.owner = value
                elif key == 'job_state' :
                    job.state = _pbs_get_state(value)
                elif key == 'queue' :
                    job.queue = value
                elif key == 'server' :
                    # do we need to get this? we already have server name from jid
                    job.scheduler_host = value
                elif key == 'qtime' :
                    job.submittime = datetime.fromtimestamp(time.mktime(time.strptime(value, '%a %b %d %H:%M:%S %Y')))
                elif key == 'stime' :
                    job.starttime = datetime.fromtimestamp(time.mktime(time.strptime(value, '%a %b %d %H:%M:%S %Y')))
                elif key == 'Account_Name' :
                    job.account = value
                elif key == 'Resource_List.ncpus' :
                    job.np = int(value)
                elif key == 'exec_host' :
                    # host here is not very accurate
                    # I don't sure how to extract host list for array job
                    # anyways, host list is here
                    job.host = map(lambda s: s.split('/', 1)[0], value.split('+'))
                elif key == 'array_indices_submitted' :
                    start, end = map(lambda s: int(s), value.split('-', 1))
                    array_all = (start, end)
                elif key == 'array_indices_remaining' :
                    start, end = map(lambda s: int(s), value.split('-', 1))
                    array_waiting = (start, end)
                    
        if not (job.jid is None) : 
            add_job(array_all, array_waiting)

        return jobs

    def submit(self, script, opts = {}, **kw) :
        '''
        Submit a job

        @type script string
        @param script job script
        @type opts dictionary
        @param opts list of options pass to scheduler, may be encoded in job script
        @rtype string
        @return single job id
        '''
        options = dict(self.default_options)
        options.update(opts)
        pbs_opts = kw.get('pbs_opts', [])
        shell_str = '#!/bin/sh\n'
        # translate PBS options into job script
        if options.has_key('shell') :
            pbs_opts.append('-S ' + options['shell'])
            shell_str = '#!' + options['shell'] + '\n'
        # np
        if options.has_key('np') and (options['np'] > 1):
            if options.has_key('type') and (options['type'] == 'mpi') :
                pbs_opts.append('-l ncpus=%d' % options['np'])
            else :
                # assume array job
                pbs_opts.append('-J 1-%d' % options['np'])
        # name
        if options.has_key('name') :
            pbs_opts.append('-N ' + options['name'][0:15])

        # environment
        if options.has_key('env') :
            for key, value in options['env'].iteritems() :
                pbs_opts.append('-v %s="%s"' % (key, value))

        # join output
        if options.has_key('join_output') and options['join_output'] :
            pbs_opts.append('-j oe')

        # dependencies
        if options.has_key('depend') :
            dependencies = ','.join(options['depend'])
            pbs_opts.append('-W depend=s' % dependencies)
            
        # output and error file
        # PBS has a little bug in task_id specification here
        if options.has_key('output') :
            output = options['output'].replace('@task_id@', 'TASK_ID')
            pbs_opts.append('-o %s' % output)
        if options.has_key('error') :
            error = options['error'].replace('@task_id@', 'TASK_ID')
            pbs_opts.append('-e %s' % error)

        # queue name
        if options.has_key('queue') :
            pbs_opts.append('-q %s' % options['queue'])

        # accounting
        if options.has_key('account') :
            pbs_opts.append('-A %s' % options['account'])

        # create PBS job script
        script_buf = StringIO()
        script_buf.write(shell_str)
        for opt in pbs_opts :
            script_buf.write(self.job_script_var('#PBS ' + opt + '\n'))
        script_buf.write('#######\n')
        
        # script
        script_buf.write(self.job_script_var(script))
        if self.logger : 
            self.logger.debug(self.name + ':submit the following job script')
            self.logger.debug(script_buf.getvalue())

        if not self.dry_run :
            sudo_cmd = kw.get('sudo', '')
            sudo_cmd = sudo_cmd + ' '
            qsub = popen2.Popen4(sudo_cmd + self.qsub)
            # parse job id
            qsub.tochild.write(script_buf.getvalue())
            qsub.tochild.close()
            line = qsub.fromchild.read()
            m = qsub_re.match(line.strip())
            if m :
                jid = m.group('jid')
            else :
                raise OSError(line)
            qsub.wait()
        else :
            jid = '0'
        script_buf.close()
        return jid

    def submit_bulk(self, script, ntask, opts = {}, **kw) :
        '''
        Submit bulk (or array) job

        @type script string
        @param script job script
        @type ntask integer
        @param ntask number of task(s) to submit
        @type opts dictionary
        @param opts list of options pass to scheduler, may be encoded in job script
        @rtype string
        @return single job id
        '''
        kw['pbs_opts'] = ['-J 1-%d' % (ntask)]
        return self.submit(script, opts, **kw)

    def cancel(self, jid_list, **kw) :
        '''
        Remove a job from queue

        @type jid_list list of string
        @param jid_list list of job id
        @rtype list of string
        @return list of successfully removed job id
        NOTE: To delete task, specify jobid.taskid or jobid/taskid
        '''
        # SGE takes task in the form of jobid.taskid
        for i in range(len(jid_list)) :
            fields = jid_list[i].split('/.', 2)
            if len(fields) >= 2 :
                jid_list[i] = '%s[%s]' % (str(fields[0]), str(fields[1]))
        # just concat the string and delete the task
        jid_args = ' '.join(jid_list)
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        cmd = sudo_cmd + self.qdel + ' ' + jid_args
        #self.logger.debug('%s:cancel cmd = %s' % (self.name, cmd) )
        #print '%s:cancel cmd = %s' % (self.name, cmd)
        qdel = popen2.Popen4(cmd)
        retval = []
        line = qdel.fromchild.readline()
        while line :
            m = qdel_re.match(line)
            if m :
                retval.append(m.group('jid'))
            line = qdel.fromchild.readline()
        qdel.wait()
        return retval

    def status(self, jid, **kw) :
        '''
        Fetch job status of specified jid

        @type jid string
        @param jid job id
        @rtype JobInfo object
        @return Job status in form of JobInfo object, None if job does not exist
        '''
        retval = self.list(jid)
        if retval :
            return retval[0]
        else :
            return None

    def hosts(self, **kw) :
        '''
        return list of hosts managed by PBS

        @rtype (list of Host, list of Host)
        @return list of (online-hosts, offline-hosts)
        '''
        sudo_cmd = kw.get('sudo', '')
        ret_type = kw.get('return_type', types.ListType)
        sudo_cmd = sudo_cmd + ' '
        pbsnodes = popen2.Popen4(sudo_cmd + self.pbsnodes + ' -a')
        host_data = {}
        if ret_type == types.ListType :
            onlines = []
            offlines = []
        else :
            retval = {}

        def add_host() :
            if ret_type == types.ListType :
                if host_data['state'] == 'up' :
                    onlines.append(Host(**host_data))
                else :
                    offlines.append(Host(**host_data))
            else :
                retval[host_data['name']] = Host(**host_data)

        cur_host = ''
        while 1 :
            line = pbsnodes.fromchild.readline()
            if not line : break
            line = line.strip()
            # blank line?
            if not line : continue
            value = line.split('= ')
            if len(value) == 1 :
                # add old data, if exists
                if host_data :
                    add_host()                       
                cur_host = value[0]
                # FIXME: Loadavg is neglected here
                host_data = {'loadavg':0.0, 'name':cur_host}
            else :
                key = value[0].strip()
                value = value[1].strip()

                if key == 'np' :
                    host_data['np'] = int(value)
                    host_data['slot_total'] = int(value)
                elif key == 'jobs' :
                    host_data['slot_used'] = len(','.split(value))
                elif key == 'state' :
                    if value == 'free' or value == 'job-exclusive' :
                        host_data['state'] = 'up'
                    else :
                        host_data['state'] = 'down'
        if host_data :
            add_host()
        pbsnodes.wait()
        if ret_type == types.ListType :
            return (onlines, offlines)
        else :
            return retval

    def queues(self, **kw) :
        '''
        Return list of queues,hosts, and slots associate with each queue

        @rtype list of Queue
        @return lits of Queues available in this system
        '''
        # run host to get list of host first
        kw['return_type'] = types.DictType
        all_hosts = self.hosts(**kw)

        # run qstat to get list of queue and load
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        cmd = popen2.Popen4(sudo_cmd + '%s -Q -f' % self.qstat)
        queues = []
        skip = 0
        have_aclhost = False
        queue_data = {}

        def add_queue(data) :
            if not data.has_key('slot_total') :
                # assume unlimited
                # FIXME: Which number should I use?
                data['slot_total'] = 9999999
            if not data.has_key('online_hosts') and not data.has_key('offline_hosts') :
                data['online_hosts'] = []
                data['offline_hosts'] = []
                for name, host in all_hosts.iteritems() :
                    if host.state == 'up' :
                        data['online_hosts'].append(host)
                    else :
                        data['offline_hosts'].append(host)
            queues.append(Queue(**data))

        while 1:
            line = cmd.fromchild.readline()
            if not line : break
            m = qstat_qname_re.match(line)
            # blank line
            blank_line = not line.strip()
            if blank_line : continue
            # check for header (qname)
            if m :
                # qname line
                skip = 0
                if queue_data :
                    add_queue(queue_data)
                    queue_data = {}
                    have_aclhost = False
                queue_data['name'] = m.group('qname').strip()
            else :
                m = qstat_qattr_re.match(line)
                if not m: continue
                if skip: continue
                # attributes line
                key = m.group('key')
                value = m.group('value').strip()
                if key == 'queue_type' :
                    if value != 'Execution' :
                        # FIXME: Ignore routing queue for now
                        queue_data = {}
                        skip = 1
                elif key == 'total_jobs' :
                    queue_data['slot_used'] = int(value)
                elif key == 'enabled' or key == 'started' :
                    if value == 'False' :
                        queue_data['state'] = 'hold'
                    else :
                        queue_data['state'] = 'active'
                elif key == 'max_running' :
                    queue_data['slot_total'] = int(value)
                elif key == 'acl_host_enable' :
                    have_aclhost = True
                elif key == 'acl_hosts' :
                    queue_data['online_hosts'] = []
                    queue_data['offline_hosts'] = []
                    if have_aclhost :
                        host_list = value.split(',')
                        for host in host_list :
                            #if not all_hosts.has_key(host) : continue
                            if not all_hosts.has_key(host) : continue
                            if all_hosts[host].state == 'up' :
                                queue_data['online_hosts'].append(all_hosts[host])
                            else :
                                queue_data['offline_hosts'].append(all_hosts[host])
        if queue_data :
            add_queue(queue_data)

        cmd.wait()
        return queues


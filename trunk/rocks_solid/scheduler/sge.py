'''
SGE implementation
'''
import os, socket, re, time, copy
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

qsub_re = re.compile(r'Your\s+job(-array)?\s+(?P<jid>[0-9]+)[.]?(?P<tid>[0-9:-]*)\s+.*submitted', re.IGNORECASE)
qdel_re = re.compile(r'.*\s+has\s+(deleted\s*job(-array\s+task)?|registered\s+the\s+job(-array\s+task)?)\s+(?P<jid>[0-9.]+).*', re.IGNORECASE)

class Scheduler(BaseScheduler):
    def __init__(self, conf={}, **kw):
        BaseScheduler.__init__(self, conf, **kw)

        self.name = 'sge'
        self.dry_run = kw.get('dry_run', False)
        self.logger = kw.get('logger', None)

        if 'sge_root' in conf:
            os.environ['SGE_ROOT'] = conf['sge_root']
        if 'sge_cell' in conf:
            os.environ['SGE_CELL'] = conf['sge_cell']
        if 'sge_arch' in conf :
            os.environ['SGE_ARCH'] = conf['sge_arch']
        if 'sge_qmaster_port' in conf :
            os.environ['SGE_QMASTER_PORT'] = conf['sge_qmaster_port']
        if 'sge_execd_port' in conf :
            os.environ['SGE_EXECD_PORT'] = conf['sge_execd_port']

        self.job_env_vars = {
            'job_id': 'JOB_ID',
            'job_name': 'JOB_NAME',
            'task_id': 'SGE_TASK_ID',
        }

        # default path to SGE
        bin_dir = os.path.join(os.environ['SGE_ROOT'], 'bin')
        for bin in ['qstat', 'qsub', 'qdel', 'qhost'] :
            path = os.path.join(bin_dir, os.environ['SGE_ARCH'], bin)
            if os.access(path, os.X_OK) :
                setattr(self, bin, path)
            else :
                setattr(self, bin, bin)
        if conf.has_key('sge_qstat') :
            self.qstat = conf['sge_qstat']
        if conf.has_key('sge_qsub') :
            self.qsub = conf['sge_qsub']
        if conf.has_key('sge_qdel') :
            self.qdel = conf['sge_qdel']
        self.extend_status = conf.get('extend_status', False)
        self.extend_host = conf.get('extend_host', False)
        self.scheduler = 'sge'
        # address of SGE master
        act_qmaster_config = os.path.join(os.environ['SGE_ROOT'], os.environ['SGE_CELL'], 'common', 'act_qmaster')
        if os.access(act_qmaster_config, os.R_OK) :
            f = open(act_qmaster_config, 'r')
            self.scheduler_host = f.read().strip()
            f.close()
        else :
            # FIXME: assume current host hold the jobs
            self.scheduler_host = socket.gethostname()

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
        extend_host = kw.get('extend_host', self.extend_host)
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        cmd = popen2.Popen4(sudo_cmd + '%s -u \* -xml' % self.qstat)
        info = ET.parse(cmd.fromchild)
        cmd.wait()

        jobs = []
        queue_info = info.find('queue_info')

        for j in queue_info.getiterator('job_list'):
            jid = j.findtext('JB_job_number')
            tid = j.findtext('tasks')
            if tid :
                jid = jid + '.' + tid
            if filter and not self._job_filter(filter, jid) :
                continue
            jobs.extend(self.parse_job_list(j, **kw))
        job_info = info.find('job_info')
        for j in job_info.getiterator('job_list'):
            jid = j.findtext('JB_job_number')
            tid = j.findtext('tasks')
            if tid :
                jid = jid + '.' + tid
            if filter and not self._job_filter(filter, jid) :
                continue
            jobs.extend(self.parse_job_list(j, **kw))
        # getting host info, if needed

        if extend_host :
            jid_host_dict = {}
            cmd = popen2.Popen4(sudo_cmd + '%s -u \* -f' % self.qstat)
            first_line = True
            ignore = False
            while 1 :
                line = cmd.fromchild.readline()
                if not line :
                    break
                if ignore :
                    continue
                # ignore first line
                if first_line :
                    first_line = False
                    continue
                if line.startswith('-----') or not line.strip():
                    continue
                # ignore all pending job, we only need host info.
                if line.startswith('#####') :
                    ignore = True
                    continue
                if not line.startswith(' ') :
                    # queue info
                    qinfo = line.split()
                    qname,host = qinfo[0].split('@')
                else :
                    # job info
                    line = line.strip()
                    jinfo = line.split()
                    jid = jinfo[0]
                    tid = None
                    if len(jinfo) == 9 :
                        tid = line[8]
                    if tid :
                        jid = jid + '.' + tid
                    # use host information from prior line
                    if not jid_host_dict.has_key(jid) :
                        jid_host_dict[jid] = []
                    jid_host_dict[jid].append(host) 
            cmd.wait()
            for job in jobs :
                jid = job.jid
                if job.tid :
                    jid = jid + '.' + job.tid
                if jid_host_dict.has_key(jid) :
                    job.host = jid_host_dict[jid]

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
        sge_opts = kw.get('sge_opts', ['-cwd'])
        shell_str = '#!/bin/sh\n'
        # translate SGE options into job script
        if options.has_key('shell') :
            sge_opts.append('-S ' + options['shell'])
            shell_str = '#!' + options['shell'] + '\n'
        # np
        if options.has_key('np') and (options['np'] > 1):
            if options.has_key('type') and (options['type'] == 'mpi') :
                sge_opts.append('-pe mpi %d' % options['np'])
            else :
                # assume array job
                sge_opts.append('-t %d' % options['np'])
        # name
        if options.has_key('name') :
            sge_opts.append('-N ' + options['name'])

        # environment
        if options.has_key('env') :
            for key, value in options['env'].iteritems() :
                sge_opts.append('-v %s="%s"' % (key, value))

        # join output
        if options.has_key('join_output') and options['join_output'] :
            sge_opts.append('-j y')

        # dependencies
        if options.has_key('depend') :
            dependencies = ','.join(options['depend'])
            sge_opts.append('-hold_jid %s' % dependencies)
            
        # output and error file
        # SGE has a little bug in task_id specification here
        if options.has_key('output') :
            output = options['output'].replace('@task_id@', 'TASK_ID')
            sge_opts.append('-o %s' % output)
        if options.has_key('error') :
            error = options['error'].replace('@task_id@', 'TASK_ID')
            sge_opts.append('-e %s' % error)

        # queue name
        if options.has_key('queue') :
            sge_opts.append('-q %s' % options['queue'])

        # accounting
        if options.has_key('account') :
            sge_opts.append('-A %s' % options['account'])

        # create SGE job script
        script_buf = StringIO()
        script_buf.write(shell_str)
        for opt in sge_opts :
            script_buf.write(self.job_script_var('#$ ' + opt + '\n'))
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
        kw['sge_opts'] = ['-t 1-%d' % (ntask), '-cwd']
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
            jid_list[i] = jid_list[i].replace('/', '.')
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

    def get_job_range(self, job_range) :
        '''
        convert SGE jobrange in to a list
        @type job_range string
        @param job_range string represent job list
        @rtype list list of integer
        @return list of job id (for iteration)
        NOTE: example of job-range: 1,3-10:1
        '''
        tasks = job_range.split(':', 1)[0]
        retval = []
        begin = 0
        end = 0
        start_val = None
        max_len = len(tasks)
        while end < max_len :
            if tasks[end] == ',' :
                value = tasks[begin:end]
                retval.append(value)
                begin = end + 1
            elif tasks[end] == '-' :
                start_val = int(tasks[begin:end])
                begin = end = end + 1
                while end < max_len :
                    if tasks[end] == ',' or tasks[end] == '-' :
                        break
                    end = end + 1
                end_val = int(tasks[begin:end])
                begin = end + 1
                retval.extend(map(str, range(start_val, (end_val + 1))))
            end = end + 1
        if begin < end :
            retval.append(tasks[begin:end])
        return retval

    def parse_job_list(self, j, **kw):
        '''
        Parse list of job, in XML format, create job info object

        @type j ElementTree iterator
        @param j iterator of ElementTree
        @rtype JobInfo object
        @return single JobInfo object 
        '''
        job_construct = kw.get('job', JobInfo)
        extend_status = kw.get('extend_status', self.extend_status)
        retval = []
        ji = job_construct()
        ji.state = state_map.get(j.get('state'), 'error')
        ji.jid = j.findtext('JB_job_number')
        ji.name = j.findtext('JB_name')
        ji.owner = j.findtext('JB_owner')
        ji.host = []
        # separate queue name and hosts here
        # may not be accurate for parallel job
        qinfo = j.findtext('queue_name').split('@', 1)
        ji.queue = qinfo[0]
        if len(qinfo) == 2 :
            ji.host = [qinfo[1]]
        ji.submittime = j.findtext('JB_submission_time')
        ji.starttime = j.findtext('JAT_start_time')
        if not ji.submittime :
            # for running job
            ji.submittime = ji.starttime
        ji.submittime = datetime.fromtimestamp(time.mktime(time.strptime(ji.submittime, '%Y-%m-%dT%H:%M:%S')))
        ji.np = j.findtext('slots')
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        if extend_status :
            # prevent "invalid schema" problem in SGE xml output
            cmdline = '%s -xml -j %s | sed "s/reported usage/reported_usage/g"' % (self.qstat, ji.jid)
            cmd = popen2.Popen4(sudo_cmd + cmdline)
            info = ET.parse(cmd.fromchild)
            cmd.wait()
            ji.account = info.findtext('JB_account')
            ji.submittime = info.findtext('JB_submission_time')
            ji.starttime = info.findtext('JAT_start_time')
        else :
            ji.account = ''
        ji.scheduler = self.scheduler
        ji.scheduler_host = self.scheduler_host
        task_id = j.findtext('tasks')
        if task_id :
            job_range = self.get_job_range(task_id)
            for i in job_range :
                tmp_ji = copy.deepcopy(ji)
                tmp_ji.tid = str(i)
                retval.append(tmp_ji)
        else :
            ji.tid = None
            retval.append(ji)

        return retval
    
    def hosts(self, **kw) :
        '''
        return list of hosts managed by SGE

        @rtype (list of Host, list of Host)
        @return list of (online-hosts, offline-hosts)
        '''
        onlines = []
        offlines = []
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        qhost = popen2.Popen4(sudo_cmd + self.qhost)
        while 1 :
            line = qhost.fromchild.readline()
            if not line :
                break
            line = line.strip()
            if line.startswith('-----') or \
                line.startswith('HOSTNAME') or \
                line.startswith('global') :
                continue
            entry = line.split()
            try :
                entry[0] = socket.getfqdn(entry[0])
            except :
                pass
            try :
                onlines.append( Host(name=entry[0], np=int(entry[2]), loadavg=float(entry[3]), state='up' ) )
            except :
                offlines.append(Host(name=entry[0], state='down'))
        return (onlines, offlines)

    def queues(self, **kw) :
        '''
        Return list of queues,hosts, and slots associate with each queue

        @rtype list of Queue
        @return lits of Queues available in this system
        '''
        # run qstat to get list of queue and load
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        cmd = popen2.Popen4(sudo_cmd + '%s -g c -xml' % self.qstat)
        queue_info = ET.parse(cmd.fromchild)
        cmd.wait()
        queues = []
        for j in queue_info.getiterator('cluster_queue_summary'):
            name = j.findtext('name')
            try :
                load = float(j.findtext('load'))
            except :
                load = 0
            used = int(j.findtext('used'))
            total = int(j.findtext('total'))
            total = total - int(j.findtext('temp_disabled')) - int(j.findtext('manual_intervention'))
            queues.append(Queue(name=name, loadavg=load, slot_used=used, slot_total=total, state='hold'))

        del queue_info
        # list each queue's host
        sudo_cmd = kw.get('sudo', '')
        sudo_cmd = sudo_cmd + ' '
        cmd = popen2.Popen4(sudo_cmd + '%s -F -xml' % self.qstat)
        queue_info = ET.parse(cmd.fromchild)
        cmd.wait()
        ref_queue = None
        for info in queue_info.getiterator('Queue-List') :
            name = info.findtext('name')
            qname, hname = name.split('@', 1)
            used = int(info.findtext('slots_used'))
            total = int(info.findtext('slots_total'))
            str_state = info.findtext('state', '')
            load = 0.0
            if 'u' in str_state :
                state = 'down'
            else :
                state = 'up'
                # Overloaded host
                for s in str_state :
                    if s in 'cE' :
                        state = 'error'
                        break
                # load average is only available is host is up
                for r in info.getiterator('resource') :
                    if r.get('name') == 'load_avg' :
                        load = r.text
                        break
                load = float(load)

            if (not ref_queue) or (ref_queue.name != qname) :
                for queue in queues :
                    if queue.name == qname :
                        ref_queue = queue
                        break
            if state != 'down' :
                if not ref_queue.online_hosts :
                    ref_queue.online_hosts = []
                ref_queue.online_hosts.append(Host(name=hname,slot_used=used,slot_total=total,loadavg=load,state=state))
            else :
                if not ref_queue.offline_hosts :
                    ref_queue.offline_hosts = []
                ref_queue.offline_hosts.append(Host(name=hname,slot_used=used,slot_total=total,loadavg=load,state=state))
        del queue_info
        return queues

# vim: ft=python ts=4 sw=4 sta et sts=4 ai:

if __name__ == '__main__' :
    import sys
    sys.path.insert(0, '../../')

    sge = Scheduler({'default_options':{'queue':'exclusive.q'}}, dry_run = False)
    script = '''\
/bin/hostname
echo "job-id = $@job_id@"
echo "task-id = $@task_id@"
sleep 1000
echo "ENVIRONMENT"
echo "==========="
env
'''
    options = {
        'name':'testjob_animagrid',
        'output': os.path.join(os.getcwd(),'output.txt'),
        'error': os.path.join(os.getcwd(),'error.txt'),
    }
    print sge.hosts()
#    print sge.get_job_range('1-3,5-8,10:1')
#    job_list = sge.list(['180774'])
#    for job in job_list :
#        print job
#    jid = sge.submit_bulk(script, 10, options)
#    print 'submit jobid = %s' % jid
#    print sge.status(jid)
#    uid = os.getuid()
#    if uid != 0 :
#        # we can try this
#        print 'deleting job %s' % ' '.join([jid, job_list[0].jid])
#        print 'deleted job %s' % sge.cancel([jid + '.2', job_list[0].jid])
#        print 'one job shouldn\'t be deleted, which is correct'
#    else :
#        print 'deleting job %s' % jid
#        print 'deleted job %s' % sge.cancel([jid])
    #online, offline = sge.hosts()
    #for ent in (online + offline) :
    #    print ent
    #queues = sge.queues()
    #for q in queues :
    #    sys.stdout.write(repr(q))

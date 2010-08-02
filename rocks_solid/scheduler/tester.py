#!/usr/bin/env python

import os, sys

mod_name = ''
if len(sys.argv) < 2 :
    sys.exit()
mod_name = sys.argv[1]

mod = __import__(mod_name)
scheduler_func = getattr(mod, 'Scheduler')

def info(arg) :
    print 'INFO:' + arg

info('Initializing scheduler')
sched = scheduler_func(dry_run = False)

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

# hosts
print
info('Getting list of managed hosts')
print sched.hosts()

# queues
print
info('Getting list of queues')
queues = sched.queues()
print queues

# list
print
info('List job')
job_list =  sched.list()
for job in job_list :
    print job

# extended list
'''
print
info('Extended job list')
job_list = sched.list(extend_status=True)
for job in job_list :
    print job
'''

# extended host
print
info('Extend_host job list')
job_list = sched.list(extend_host=True)
for job in job_list :
    print job


print
info('List partial job')
filter = map(lambda s: s.jid, job_list)
for job in sched.list(filter[0:1]) :
    print job

print
info('Test job submission')
jid = sched.submit(script, options)
info('Job submitted. Got job id %s' % jid)
#print sge.get_job_range('1-3,5-8,10:1')
#job_list = sge.list(['180774'])
#for job in job_list :
#    print job
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

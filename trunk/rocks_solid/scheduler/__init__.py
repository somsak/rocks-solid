'''
Scheduler essential class
'''

import types, socket

# vim: ft=python ts=4 sw=4 sta et sts=4 ai:
job_state = ['waiting','running','error','finished']
queue_state = ['active', 'hold']
host_state = ['up', 'down', 'error']

class BaseScheduler(object) :
    '''
    Base scheduler class
    '''
    def __init__(self, conf={}, **kw) :
        '''
        Initialization
        '''
        self.name = ''
        self.default_options = conf.get('default_options', {})
        self.job_env_vars = {}

    def submit(self, script, opts = {}, **kw) :
        pass

    def submit_bulk(self, script, ntask, opts = {}, **kw) :
        pass

    def list(self, filter = None, **kw):
        pass

    def status(self, jid, **kw):
        pass

    def cancel(self, jid_list, **kw):
        pass

    def hosts(self, **kw) :
        '''
        This function only give a list of host(s) managed by this scheduler
        slot used / total may or may not be set for this function
        '''
        pass

    def queues(self, **kw) :
        '''This function will give both list of queue and host associated with each queue'''
        pass

    def job_script_var(self, script) :
        '''
        Substitute special character in job script with scheduler-specific job script environment

        @type script string
        @param script input job script
        @rtype string
        @return patched job script
        NOTE: rely on self.job_env_vars
        '''
        new_script = script
        for key, value in self.job_env_vars.iteritems() :
            new_script = new_script.replace('@' + key + '@', value)
        return new_script

class Host(object) :
    def __init__(self, **kw) :
        self.name = kw.get('name', '')
        # slot used and total is used to get number of job running in the host
        self.slot_used = kw.get('slot_used', 0)
        self.slot_total = kw.get('slot_total', 0)
        self.np = kw.get('np', 0)
        self.loadavg = kw.get('loadavg', 0)
        self.set_state(kw.get('state', 'down'))

    def get_state(self) :
        return self._state
    def set_state(self, state) :
        assert state in host_state
        self._state = state
    state = property(get_state, set_state)

    def __repr__(self) :
        return '<Host %(name)s,%(np)d,%(slot_used)d/%(slot_total)d,%(_state)s,%(loadavg).1f>' % vars(self)

    def __eq__(self, other) :
        # if host is logically equal (by ip address and name).
        name = None
        if type(other) == types.StringType or type(other) == types.UnicodeType :
            name = other
        elif type(other) == type(self) :
            if hasattr(other, 'name') :
                name = other.name
        if name is None :
            return False
        try :
            result1 = socket.getfqdn(self.name)
            result2 = socket.getfqdn(name)
            return result1 == result2
        except :
            return False

    def __ne__(self, other) :
        return not self.__eq__(other)

class Queue(object) :
    def __init__(self, **kw) :
        self.name = kw.get('name', '')
        self.slot_used = kw.get('slot_used', 0)
        self.slot_total = kw.get('slot_total', 0)
        self.loadavg = kw.get('loadavg', 0)
        self.set_online_hosts(kw.get('online_hosts', None))
        self.set_offline_hosts(kw.get('offline_hosts', None))
        self.set_state(kw.get('state', 'active'))

    def get_state(self) :
        return self._state
    def set_state(self, state) :
        assert state in queue_state
        self._state = state
    state = property(get_state, set_state)

    def get_online_hosts(self) :
        return self._online_hosts
    def set_online_hosts(self, online_hosts) :
        self._online_hosts = online_hosts
    online_hosts = property(get_online_hosts, set_online_hosts)

    def get_offline_hosts(self) :
        return self._offline_hosts
    def set_offline_hosts(self, offline_hosts) :
        self._offline_hosts = offline_hosts
    offline_hosts = property(get_offline_hosts, set_offline_hosts)

    def __repr__(self) :
        retval = '<Q %(name)s,%(_state)s,%(slot_used)d,%(slot_total)d,%(loadavg).1f>' % vars(self)
        if self._online_hosts :
            for host in self._online_hosts :
                retval = retval + '\n\tOn:%s' % str(host)
        if self._offline_hosts :
            for host in self._offline_hosts :
                retval = retval + '\n\tOff:%s' % str(host)
        retval = retval + '\n'
        return retval

class JobInfo(object):
    def __init__(self, **kw):
        self.jid = kw.get('jid', None)
        self.tid = kw.get('tid', None)
        self.name = kw.get('name', '')
        self.owner = kw.get('owner', '')
        self.queue = kw.get('queue', '')
        self.account = kw.get('account', '')

        self._np = kw.get('np', 1)
        if kw.has_key('np') :
            self.set_np(kw['np'])
        self._state = 'waiting'
        if kw.has_key('state') :
            self.set_state(kw['state'])

        self.host = None
        self.submittime = kw.get('submittime', None)
        self.starttime = kw.get('starttime', None)
        self.scheduler = kw.get('scheduler', '')
        self.scheduler_host = kw.get('scheduler_host', '')

    def __repr__(self):
        return '<job %s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s>' % \
                (self.jid,self.tid,self.name,self.owner,self.queue,self.np,self.state,self.scheduler,self.scheduler_host, self.host,self.submittime,self.starttime)

    def get_np(self):
        return self._np
    def set_np(self,v):
        self._np = int(v)
    np = property(get_np, set_np)

    def get_state(self):
        return self._state
    def set_state(self,v):
        assert v in job_state
        self._state = v
    state = property(get_state, set_state)

def jidparse(jid_str) :
    pass

def jidunparse(tuple) :
    pass

if __name__ == '__main__' :
    h1 = Host(name = 'compute-0-0')
    h2 = Host(name = 'compute-0-1')
    h3 = Host(name = 'compute-0-x')
    #Queue(name = 'test')
    #j = JobInfo()
    print 'compute-0-0 == compute-0-1 : ', h1 == h2
    print 'compute-0-1 == compute-0-0 : ', h2 == h1
    print 'compute-0-0 == compute-0-0 : ', h1 == h1
    print 'compute-0-0 == compute-0-0 (string) : ', h1 == 'compute-0-0'
    print 'compute-0-x == compute-0-0 (string) : ', h3 == 'compute-0-0'

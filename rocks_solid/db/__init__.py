'''
Database interface
'''

import types
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, ForeignKey   
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.sql import and_, or_, not_

metadata = MetaData()

host_status = Table('status', metadata,
        Column('id', Integer, primary_key = True),
        Column('name', String(255), nullable = False, index = True),
        Column('on_time', DateTime, nullable = False, index = True),
        Column('off_time', DateTime, nullable = True, index = True, default = None),
        Column('desc', String(255))
    )

class HostStatus(object) :
    def __init__(self, **kw) :
        self.id = kw.get('id', None)
        self.name = kw.get('name', '')
        self.on_time = kw.get('on_time', None)
        self.off_time = kw.get('off_time', None)
        self.desc = kw.get('desc', '')

    def __repr__(self) :
        return '<HostStatus(%d,%s,%s,%s)>' % (self.id, self.name, self.on_time, self.off_time)

    def __cmp__(self, other) :
        if type(other) == types.StringTypes :
            return self.name.__cmp__(other)
        elif type(other) == type(self) :
            return (self.id.__cmp__(other) and self.name.__cmp__(other))

mapper(HostStatus, host_status)

host_event = Table('event', metadata,
        Column('id', Integer, primary_key = True),
        Column('name', String(255), nullable = False, index = True),
        Column('event', String(255), nullable = False),
        Column('time', DateTime, nullable = False),
        Column('status', String(255), nullable = True, default = None, index = True)
    )

class HostEvent(object) :
    def __init__(self, **kw) :
        self.id = kw.get('id', None)
        self.name = kw.get('name', '')
        self.event = kw.get('event', '')
        self.time = kw.get('time', None)
        self.status = kw.get('status', None)

    def __repr__(self) :
        return '<HostEvent(%d,%s,%s,%s,%s)>' % (self.id, self.name, self.event, self.time, self.status)

mapper(HostEvent, host_event)

class DB(object) :
    '''
    Generic database object for Rocks-solid host database

    In general, on line hosts will ALWAYS in database, leaving 'off' record blank
    Offline hosts will only update blank 'off' record in database with new status
    '''
    auto_on = 'auto_on'
    auto_off = 'auto_off'
    success = 'success'
    failed = 'failed'

    on_events = [
        auto_on,
    ]

    off_events = [
        auto_off,
    ]

    def __init__(self, **kw) :
        self.db_url = kw.get('url', None)
        if not self.db_url :
            raise IOError('Database url is needed')
        self.verbose = kw.get('verbose', False)
        self.engine = create_engine(self.db_url, echo=self.verbose)
        self.init_db()

    def init_db(self) :
        '''
        Initialize database table and attributes
        '''
        metadata.create_all(self.engine)

    def sync_host_status(self, onlines, offlines) :
        '''
        Synchronize online/offline host status into database

        @type onlines list of scheduler.Host object
        @param onlines list of on-line host name
        '''
        Session = sessionmaker(bind=self.engine, autoflush=True, transactional=True)
        session = Session()
        # query for any host entry with 'off' record
        result = session.query(HostStatus).filter(HostStatus.off_time == None)
        # put host that's still in host_list into database
        # build offline hosts
        # in case we miss something (host downed by administrative request)
        now = datetime.today()
        for host_act in result :
            #print 'host_act = ', host_act
            if host_act.name in offlines :
                host_act.off_time = now
                host_act.desc = 'off detected'
                #print host_act.name
                #session.save(host_act)
            else :
                onlines.remove(host_act.name)
                
        # build new on-line hosts
        for host in onlines :
            session.save(HostStatus(name=host.name, on_time=now, desc='on detected'))
        session.commit()

    def update_hosts(self, host_list, state = 'off') :
        '''
        Update off_time in list of hosts (assume hosts is already in db) 

        @type host_list list of string
        @param host_list list of host name to update
        @type state string
        @param state status of host to set
        '''
        # update all entry each host in host_list, set off_time
        conn = self.engine.connect()
        for host in host_list :
            if state == 'off' :
                conn.execute(host_status.update(and_(host_status.c.name == host, host_status.c.off_time == None)), off_time = datetime.now(), desc = 'manual off')
            else :
                conn.execute(host_status.insert(values={'name':host, 'on_time':datetime.now(), 'desc':'manual on'}))

    def insert_event(self, host_list, event) :
        '''
        Insert host event into database

        @type host_list list of string
        @param host_list list of host name to insert to db
        @type event string
        @param event event (auto_on, auto_off, etc...)
        '''
        Session = sessionmaker(bind=self.engine, autoflush=True, transactional=True)
        session = Session()
        for host in host_list :
            session.save(HostEvent(name=host, event = event, time=datetime.today()))
        session.commit()

    def update_event(self, onlines, offlines, limit) :
        '''
        Update evet status (failed, success)
        '''
        Session = sessionmaker(bind=self.engine, autoflush=True, transactional=True)
        session = Session()
        result = session.query(HostEvent).filter(HostEvent.status == None)
        now = datetime.today()
        limit_delta = timedelta(0, limit)
        for event in result :
            # check on-line hosts, regardless of limit
            #print event
            if ((event.event == self.auto_on) and (event.name in onlines)) or \
               ((event.event == self.auto_off) and (event.name in offlines)) :
                event.status = self.success
            elif ((now - event.time) > limit_delta) :
                if ((event.event == self.auto_on) and (event.name in offlines)) or \
                    ((event.event == self.auto_off) and (event.name in onlines)) :
                    event.status = self.failed
                else :
                    event.status = self.success
        session.commit()

if __name__ == '__main__' :
    import os, time

    db_path = 'sqlite:///%s/test.sqlite' % '/tmp'
    #db_path = 'sqlite:///:memory:'
    #db_path = 'mysql://stat:1q2w3e4r@localhost/node_status'
    print db_path
    db = DB(url=db_path, verbose=True)
    db.sync_host_status(['compute-0-1.local', 'compute-0-2.local', 'compute-0-3.local'])
    db.update_hosts(['compute-0-1.local', 'compute-0-4.local'], 'off')
    db.update_hosts(['compute-0-5.local'], 'on')
    db.insert_event(['compute-0-5.local', 'compute-0-1.local'], 'auto_on')
    time.sleep(2)
    db.update_event(['compute-0-5.local'], ['compute-0-1.local'], 1)
    print db.auto_on

'''
Database interface
'''

import types
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, ForeignKey   
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.sql import and_, or_, not_

metadata = MetaData()

host_status = Table('status', metadata,
        Column('id', Integer, primary_key = True),
        Column('name', String(255), nullable = False),
        Column('on_time', DateTime, nullable = False),
        Column('off_time', DateTime),
        Column('on_comment', String),
        Column('off_comment', String)
    )

class HostActivity(object) :
    def __init__(self, **kw) :
        self.id = kw.get('id', None)
        self.name = kw.get('name', '')
        self.on_time = kw.get('on_time', None)
        self.off_time = kw.get('off_time', None)
        self.on_comment = kw.get('on_comment', '')
        self.off_comment = kw.get('off_comment', '')

    def __repr__(self) :
        return '<HostAct(%d,%s,%s,%s)>' % (self.id, self.name, self.on_time, self.off_time)

    def __cmp__(self, other) :
        if type(other) == types.StringTypes :
            return self.name.__cmp__(other)
        elif type(other) == type(self) :
            return (self.id.__cmp__(other) and self.name.__cmp__(other))

mapper(HostActivity, host_status)

class DB(object) :
    '''
    Generic database object for Rocks-solid host database

    In general, on line hosts will ALWAYS in database, leaving 'off' record blank
    Offline hosts will only update blank 'off' record in database with new status
    '''
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

    def insert_on_hosts(self, host_list) :
        '''
        Put the list of specified hosts to database

        @type host_list list of string
        @param host_list list of host name
        '''
        Session = sessionmaker(bind=self.engine, autoflush=True, transactional=True)
        session = Session()
        # query for any host entry with 'off' record
        result = session.query(HostActivity).filter(HostActivity.off_time == None)
        # put host that's still in host_list into database
        # build offline hosts
        # in case we miss something (host downed by administrative request)
        now = datetime.today()
        for host_act in result :
            if host_act.name not in host_list :
                host_act.off_time = now
                host_act.off_comment = 'off detected'
                #print host_act.name
                #session.save(host_act)
            else :
                host_list.remove(host_act.name)
        # build new on-line hosts
        for host in host_list :
            session.save(HostActivity(name=host, on_time=now, on_comment='on detected'))
        session.commit()

    def update_hosts(self, host_list, state = 'off') :
        '''
        Update off_time in list of hosts (assume hosts is already in db) 
        '''
        # update all entry each host in host_list, set off_time
        conn = self.engine.connect()
        for host in host_list :
            if state == 'off' :
                conn.execute(host_status.update(and_(host_status.c.name == host, host_status.c.off_time == None)), off_time = datetime.now(), off_comment = 'manual off')
            else :
                conn.execute(host_status.insert(values={'name':host, 'on_time':datetime.now(), 'on_comment':'manual on'}))

if __name__ == '__main__' :
    import os
    db_path = 'sqlite:///%s/test.sqlite' % '/tmp'
    #db_path = 'sqlite:///:memory:'
    #db_path = 'mysql://stat:1q2w3e4r@localhost/node_status'
    print db_path
    db = DB(url=db_path, verbose=True)
    db.insert_on_hosts(['compute-0-1.local', 'compute-0-2.local', 'compute-0-3.local'])
    db.update_hosts(['compute-0-1.local', 'compute-0-4.local'], 'off')
    db.update_hosts(['compute-0-5.local'], 'on')

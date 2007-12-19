'''
Database interface
'''

import types
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, ForeignKey   
from sqlalchemy.orm import mapper, sessionmaker

metadata = MetaData()

host_activity = Table('host_activity', metadata,
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

mapper(HostActivity, host_activity)

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
        self.Session = sessionmaker(bind=self.engine, autoflush=True, transactional=True)
        self._session = None

    def get_session(self) :
        if not self._session :
            self._session = self.Session()
        return self._session

    def set_session(self, session) :
        self._session = session

    session = property(get_session, set_session) 

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
        # query for any host entry with 'off' record
        result = self.session.query(HostActivity).filter(HostActivity.off_time == None)
        # build offline hosts
        # in case we miss something (host downed by administrative request)
        now = datetime.today()
        for host_acct in result :
            if host_acct.name not in host_list :
                host_acct.off_time = now
                self.session.save(host_acct)
        # build new on-line hosts
        print result
        for host in host_list :
            if host not in result :
                self.session.save(HostActivity(name=host, on_time=now, on_comment='on detected'))
        # put host that's still in host_list into database
        self.session.commit()

    def update_off_hosts(self, host_list, state) :
        '''
        Update list of hosts (assume hosts is already in db) with state
        '''
        pass

if __name__ == '__main__' :
    import os
    db_path = 'sqlite:///%s/test.sqlite' % os.getcwd()
    db_path = 'sqlite:///%s/test.sqlite' % '/tmp'
    #db_path = 'sqlite:///:memory:'
    db_path = 'mysql://stat:1q2w3e4r@localhost/node_status'
    print db_path
    db = DB(url=db_path, verbose=True)
    db.insert_on_hosts(['compute-0-0.local', 'compute-0-1.local'])

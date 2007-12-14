'''
Database interface
'''

import time
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, ForeignKey   
from sqlalchemy.orm import mapper

metadata = MetaData()

host_activity = Table('host_activity', metadata,
        Column('id', Integer, primary_key = True),
        Column('name', String(255), nullable = False),
        Column('on_time', DateTime, nullable = False),
        Column('off_time', DateTime),
        Column('comment', String)
    )

class HostActivity(object) :
    def __init__(self, **kw) :
        self.id = kw.get('id', 0)
        self.name = kw.get('name', '')
        self.on_time = kw.get('on_time', None)
        self.off_time = kw.get('off_time', None)
        self.comments = kw.get('comment', '')

    def __repr__(self) :
        return '<HostAct(%d,%s,%s,%s)>' % (self.id, self.name, time.asctime(self.on_time), time.asctime(self.off_time))

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
        try :
            metadata.create_all(self.engine)
        except :
            raise

    def init_db(self) :
        '''
        Initialize database table and attributes
        '''
        pass

    def insert_on_hosts(self, host_list) :
        '''
        Put the list of specified hosts to database

        '''
        pass

    def update_off_hosts(self, host_list, state) :
        '''
        Update list of hosts (assume hosts is already in db) with state
        '''
        pass

if __name__ == '__main__' :
    import os
    db_path = 'sqlite://%s/test.db' % os.getcwd()
    db_path = 'sqlite:///:memory:'
    print db_path
    db = DB(url=db_path, verbose=True)

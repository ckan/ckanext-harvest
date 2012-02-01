import logging
import datetime

from sqlalchemy import distinct
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import backref, relation

from ckan import model
from ckan.model.meta import (metadata,  mapper, Session,
                            Table, Column, ForeignKey, types)
from ckan.model.types import make_uuid
from ckan.model.domain_object import DomainObject
from ckan.model.package import Package





log = logging.getLogger(__name__)

__all__ = [
    'HarvestSource', 'harvest_source_table',
    'HarvestJob', 'harvest_job_table',
    'HarvestObject', 'harvest_object_table',
    'HarvestGatherError', 'harvest_gather_error_table',
    'HarvestObjectError', 'harvest_object_error_table',
]


harvest_source_table = None
harvest_job_table = None
harvest_object_table = None
harvest_gather_error_table = None
harvest_object_error_table = None

def setup():


    if harvest_source_table is None:
        define_harvester_tables()
        log.debug('Harvest tables defined in memory')

    if model.repo.are_tables_created():
        if not harvest_source_table.exists():

            # Create each table individually rather than
            # using metadata.create_all()
            harvest_source_table.create()
            harvest_job_table.create()
            harvest_object_table.create()
            harvest_gather_error_table.create()
            harvest_object_error_table.create()

            log.debug('Harvest tables created')
        else:
            from ckan.model.meta import engine
            log.debug('Harvest tables already exist')
            # Check if existing tables need to be updated
            inspector = Inspector.from_engine(engine)
            columns = inspector.get_columns('harvest_source')
            if not 'title' in [column['name'] for column in columns]:
                log.debug('Harvest tables need to be updated')
                migrate_v2()

    else:
        log.debug('Harvest table creation deferred')


class HarvestError(Exception):
    pass

class HarvestDomainObject(DomainObject):
    '''Convenience methods for searching objects
    '''
    key_attr = 'id'

    @classmethod
    def get(cls, key, default=None, attr=None):
        '''Finds a single entity in the register.'''
        if attr == None:
            attr = cls.key_attr
        kwds = {attr: key}
        o = cls.filter(**kwds).first()
        if o:
            return o
        else:
            return default

    @classmethod
    def filter(cls, **kwds):
        query = Session.query(cls).autoflush(False)
        return query.filter_by(**kwds)


class HarvestSource(HarvestDomainObject):
    '''A Harvest Source is essentially a URL plus some other metadata.
       It must have a type (e.g. CSW) and can have a status of "active"
       or "inactive". The harvesting processes are not fired on inactive
       sources.
    '''
    pass

class HarvestJob(HarvestDomainObject):
    '''A Harvesting Job is performed in two phases. In first place, the
       **gather** stage collects all the Ids and URLs that need to be fetched
       from the harvest source. Errors occurring in this phase
       (``HarvestGatherError``) are stored in the ``harvest_gather_error``
       table. During the next phase, the **fetch** stage retrieves the
       ``HarvestedObjects`` and, if necessary, the **import** stage stores
       them on the database. Errors occurring in this second stage
       (``HarvestObjectError``) are stored in the ``harvest_object_error``
       table.
    '''
    pass

class HarvestObject(HarvestDomainObject):
    '''A Harvest Object is created every time an element is fetched from a
       harvest source. Its contents can be processed and imported to ckan
       packages, RDF graphs, etc.

    '''

    @property
    def source(self):
        return self.job.source

class HarvestGatherError(HarvestDomainObject):
    '''Gather errors are raised during the **gather** stage of a harvesting
       job.
    '''
    pass

class HarvestObjectError(HarvestDomainObject):
    '''Object errors are raised during the **fetch** or **import** stage of a
       harvesting job, and are referenced to a specific harvest object.
    '''
    pass

def define_harvester_tables():

    global harvest_source_table
    global harvest_job_table
    global harvest_object_table
    global harvest_gather_error_table
    global harvest_object_error_table

    harvest_source_table = Table('harvest_source', metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('url', types.UnicodeText, nullable=False),
        Column('title', types.UnicodeText, default=u''),
        Column('description', types.UnicodeText, default=u''),
        Column('config', types.UnicodeText, default=u''),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
        Column('type',types.UnicodeText,nullable=False),
        Column('active',types.Boolean,default=True),
        Column('user_id', types.UnicodeText, default=u''),
        Column('publisher_id', types.UnicodeText, default=u''),
    )
    # Was harvesting_job
    harvest_job_table = Table('harvest_job', metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
        Column('gather_started', types.DateTime),
        Column('gather_finished', types.DateTime),
        Column('source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
        Column('status', types.UnicodeText, default=u'New', nullable=False),
    )
    # Was harvested_document
    harvest_object_table = Table('harvest_object', metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('guid', types.UnicodeText, default=u''),
        Column('current',types.Boolean,default=False),
        Column('gathered', types.DateTime, default=datetime.datetime.utcnow),
        Column('fetch_started', types.DateTime),
        Column('content', types.UnicodeText, nullable=True),
        Column('fetch_finished', types.DateTime),
        Column('metadata_modified_date', types.DateTime),
        Column('retry_times',types.Integer),
        Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
        Column('harvest_source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
        Column('package_id', types.UnicodeText, ForeignKey('package.id'), nullable=True),
    )
    # New table
    harvest_gather_error_table = Table('harvest_gather_error',metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
        Column('message', types.UnicodeText),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
    )
    # New table
    harvest_object_error_table = Table('harvest_object_error',metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('harvest_object_id', types.UnicodeText, ForeignKey('harvest_object.id')),
        Column('message',types.UnicodeText),
        Column('stage', types.UnicodeText),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
    )

    mapper(
        HarvestSource,
        harvest_source_table,
        properties={
            'jobs': relation(
                HarvestJob,
                lazy=True,
                backref=u'source',
                order_by=harvest_job_table.c.created,
            ),
        },
    )

    mapper(
        HarvestJob,
        harvest_job_table,
    )

    mapper(
        HarvestObject,
        harvest_object_table,
        properties={
            'package':relation(
                Package,
                lazy=True,
                backref='harvest_objects',
            ),
            'job': relation(
                HarvestJob,
                lazy=True,
                backref=u'objects',
            ),
        },
    )

    mapper(
        HarvestGatherError,
        harvest_gather_error_table,
        properties={
            'job':relation(
                HarvestJob,
                backref='gather_errors'
            ),
        },
    )

    mapper(
        HarvestObjectError,
        harvest_object_error_table,
        properties={
            'object':relation(
                HarvestObject,
                backref='errors'
            ),
        },
    )


def migrate_v2():
    log.debug('Migrating harvest tables to v2. This may take a while...')
    conn = Session.connection()

    statements = '''
    ALTER TABLE harvest_source ADD COLUMN title text;

    ALTER TABLE harvest_object ADD COLUMN current boolean;
    ALTER TABLE harvest_object ADD COLUMN harvest_source_id text;
    ALTER TABLE harvest_object ADD CONSTRAINT harvest_object_harvest_source_id_fkey FOREIGN KEY (harvest_source_id) REFERENCES harvest_source(id);

    UPDATE harvest_object o SET harvest_source_id = j.source_id FROM harvest_job j WHERE o.harvest_job_id = j.id;
    '''
    conn.execute(statements)

    # Flag current harvest_objects
    guids = Session.query(distinct(HarvestObject.guid)) \
            .join(Package) \
            .filter(HarvestObject.package!=None) \
            .filter(Package.state==u'active')

    update_statement = '''
    UPDATE harvest_object
    SET current = TRUE
    WHERE id = (
        SELECT o.id
        FROM harvest_object o JOIN package p ON p.id = o.package_id
        WHERE o.package_id IS NOT null AND p.state = 'active'
            AND o.guid = '%s'
        ORDER BY metadata_modified_date DESC, fetch_finished DESC, gathered DESC
        LIMIT 1)
    '''

    for guid in guids:
        conn.execute(update_statement % guid)

    conn.execute('UPDATE harvest_object SET current = FALSE WHERE current IS NOT TRUE')

    Session.commit()
    log.info('Harvest tables migrated to v2')

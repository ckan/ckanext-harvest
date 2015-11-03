import logging
import datetime
import uuid

from sqlalchemy import event
from sqlalchemy import distinct
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import types
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import backref, relation

from ckan import model
from ckan import logic
from ckan.model.meta import metadata,  mapper, Session
from ckan.model.types import make_uuid
from ckan.model.domain_object import DomainObject
from ckan.model.package import Package
from ckan.lib.munge import munge_title_to_name

UPDATE_FREQUENCIES = ['MANUAL','MONTHLY','WEEKLY','BIWEEKLY','DAILY', 'ALWAYS']

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
harvest_object_extra_table = None


def setup():

    if harvest_source_table is None:
        define_harvester_tables()
        log.debug('Harvest tables defined in memory')

    if not model.package_table.exists():
        log.debug('Harvest table creation deferred')
        return

    if not harvest_source_table.exists():

        # Create each table individually rather than
        # using metadata.create_all()
        harvest_source_table.create()
        harvest_job_table.create()
        harvest_object_table.create()
        harvest_gather_error_table.create()
        harvest_object_error_table.create()
        harvest_object_extra_table.create()

        log.debug('Harvest tables created')
    else:
        from ckan.model.meta import engine
        log.debug('Harvest tables already exist')
        # Check if existing tables need to be updated
        inspector = Inspector.from_engine(engine)
        columns = inspector.get_columns('harvest_source')
        column_names = [column['name'] for column in columns]
        if not 'title' in column_names:
            log.debug('Harvest tables need to be updated')
            migrate_v2()
        if not 'frequency' in column_names:
            log.debug('Harvest tables need to be updated')
            migrate_v3()

        # Check if this instance has harvest source datasets
        source_ids = Session.query(HarvestSource.id).filter_by(active=True).all()
        source_package_ids = Session.query(model.Package.id).filter_by(type=u'harvest', state='active').all()
        sources_to_migrate = set(source_ids) - set(source_package_ids)
        if sources_to_migrate:
            log.debug('Creating harvest source datasets for %i existing sources', len(sources_to_migrate))
            sources_to_migrate = [s[0] for s in sources_to_migrate]
            migrate_v3_create_datasets(sources_to_migrate)


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
    def __repr__(self):
        return '<HarvestSource id=%s title=%s url=%s active=%r>' % \
               (self.id, self.title, self.url, self.active)

    def __str__(self):
        return self.__repr__().encode('ascii', 'ignore')


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

class HarvestObjectExtra(HarvestDomainObject):
    '''Extra key value data for Harvest objects'''

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

def harvest_object_before_insert_listener(mapper,connection,target):
    '''
        For compatibility with old harvesters, check if the source id has
        been set, and set it automatically from the job if not.
    '''
    if not target.harvest_source_id or not target.source:
        if not target.job:
            raise Exception('You must define a Harvest Job for each Harvest Object')
        target.source = target.job.source
        target.harvest_source_id = target.job.source.id


def define_harvester_tables():

    global harvest_source_table
    global harvest_job_table
    global harvest_object_table
    global harvest_object_extra_table
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
        Column('frequency', types.UnicodeText, default=u'MANUAL'),
        Column('next_run', types.DateTime),
    )
    # Was harvesting_job
    harvest_job_table = Table('harvest_job', metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
        Column('gather_started', types.DateTime),
        Column('gather_finished', types.DateTime),
        Column('finished', types.DateTime),
        Column('source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
        # status: New, Running, Finished
        Column('status', types.UnicodeText, default=u'New', nullable=False),
    )
    # A harvest_object contains a representation of one dataset during a
    # particular harvest
    harvest_object_table = Table('harvest_object', metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        # The guid is the 'identity' of the dataset, according to the source.
        # So if you reharvest it, then the harvester knows which dataset to
        # update because of this identity. The identity needs to be unique
        # within this CKAN.
        Column('guid', types.UnicodeText, default=u''),
        # When you harvest a dataset multiple times, only the latest
        # successfully imported harvest_object should be flagged 'current'.
        # The import_stage usually reads and writes it.
        Column('current',types.Boolean,default=False),
        Column('gathered', types.DateTime, default=datetime.datetime.utcnow),
        Column('fetch_started', types.DateTime),
        Column('content', types.UnicodeText, nullable=True),
        Column('fetch_finished', types.DateTime),
        Column('import_started', types.DateTime),
        Column('import_finished', types.DateTime),
        # state: WAITING, FETCH, IMPORT, COMPLETE, ERROR
        Column('state', types.UnicodeText, default=u'WAITING'),
        Column('metadata_modified_date', types.DateTime),
        Column('retry_times',types.Integer, default=0),
        Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
        Column('harvest_source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
        Column('package_id', types.UnicodeText, ForeignKey('package.id', deferrable=True), nullable=True),
        # report_status: 'added', 'updated', 'not modified', 'deleted', 'errored'
        Column('report_status', types.UnicodeText, nullable=True),
    )

    # New table
    harvest_object_extra_table = Table('harvest_object_extra', metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('harvest_object_id', types.UnicodeText, ForeignKey('harvest_object.id')),
        Column('key',types.UnicodeText),
        Column('value', types.UnicodeText),
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
        Column('message', types.UnicodeText),
        Column('stage', types.UnicodeText),
        Column('line', types.Integer),
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
            'source': relation(
                HarvestSource,
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
                backref=backref('errors', cascade='all,delete-orphan')
            ),
        },
    )

    mapper(
        HarvestObjectExtra,
        harvest_object_extra_table,
        properties={
            'object':relation(
                HarvestObject,
                backref=backref('extras', cascade='all,delete-orphan')
            ),
        },
    )

    event.listen(HarvestObject, 'before_insert', harvest_object_before_insert_listener)

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


def migrate_v3():
    log.debug('Migrating harvest tables to v3. This may take a while...')
    conn = Session.connection()

    statement =  """CREATE TABLE harvest_object_extra (
	id text NOT NULL,
	harvest_object_id text,
	"key" text,
	"value" text
);

ALTER TABLE harvest_object
	ADD COLUMN import_started timestamp without time zone,
	ADD COLUMN import_finished timestamp without time zone,
	ADD COLUMN "state" text,
	ADD COLUMN "report_status" text;

ALTER TABLE harvest_source
	ADD COLUMN frequency text,
    ADD COLUMN next_run timestamp without time zone;

ALTER TABLE harvest_job
    ADD COLUMN finished timestamp without time zone;

ALTER TABLE harvest_object_extra
	ADD CONSTRAINT harvest_object_extra_pkey PRIMARY KEY (id);

ALTER TABLE harvest_object_extra
	ADD CONSTRAINT harvest_object_extra_harvest_object_id_fkey FOREIGN KEY (harvest_object_id) REFERENCES harvest_object(id);

UPDATE harvest_object set state = 'COMPLETE' where package_id is not null;
UPDATE harvest_object set state = 'ERROR' where package_id is null;
UPDATE harvest_object set retry_times = 0;
UPDATE harvest_object set report_status = 'updated' where package_id is not null;
UPDATE harvest_object set report_status = 'errored' where package_id is null;
UPDATE harvest_source set frequency = 'MANUAL';

ALTER TABLE harvest_object DROP CONSTRAINT harvest_object_package_id_fkey;
ALTER TABLE harvest_object
    ADD CONSTRAINT harvest_object_package_id_fkey FOREIGN KEY (package_id) REFERENCES package(id) DEFERRABLE;

ALTER TABLE harvest_object_error
	ADD COLUMN line integer;

"""
    conn.execute(statement)
    Session.commit()
    log.info('Harvest tables migrated to v3')

class PackageIdHarvestSourceIdMismatch(Exception):
    """
    The package created for the harvest source must match the id of the
    harvest source
    """
    pass

def migrate_v3_create_datasets(source_ids=None):
    import pylons
    from paste.registry import Registry

    from ckan.lib.cli import MockTranslator
    registry = Registry()
    registry.prepare()
    registry.register(pylons.translator, MockTranslator())

    sources = []
    if not source_ids:
        sources = model.Session.query(HarvestSource).all()

    else:
        sources = model.Session.query(HarvestSource) \
                  .filter(HarvestSource.id.in_(source_ids)) \
                  .all()

    if not sources:
        log.debug('No harvest sources to migrate')
        return


    site_user_name = logic.get_action('get_site_user')({'model': model, 'ignore_auth': True},{})['name']

    context = {'model': model,
               'session': model.Session,
               'user': site_user_name, # TODO: auth of existing sources?
               'return_id_only': True,
               'extras_as_string': True,
              }

    def gen_new_name(title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        pkg_obj = Session.query(Package).filter(Package.name == name).first()
        if pkg_obj:
            return name + str(uuid.uuid4())[:5]
        else:
            return name

    for source in sources:
        if 'id' in context:
            del context['id']
        if 'package' in context:
            del context['package']

        # Check if package already exists

        try:
            logic.get_action('package_show')(context, {'id': source.id})
            continue
        except logic.NotFound:
            pass

        package_dict = {
            'id': source.id,
            'name': gen_new_name(source.title) if source.title else source.id,
            'title': source.title if source.title else source.url,
            'notes': source.description,
            'url': source.url,
            'type': 'harvest',
            'source_type': source.type,
            'config': source.config,
            'frequency': source.frequency,
            }
        context['message'] = 'Created package for harvest source {0}'.format(source.id)
        try:
            new_package_id = logic.get_action('package_create')(context, package_dict)
            if new_package_id != source.id or not context['return_id_only']:
                # this check only makes sense if we are sure we are returning
                # the package id not the package object
                raise PackageIdHarvestSourceIdMismatch

            log.info('Created new package for source {0} ({1})'.format(source.id, source.url))
        except logic.ValidationError,e:
            log.error('Validation Error: %s' % str(e.error_summary))

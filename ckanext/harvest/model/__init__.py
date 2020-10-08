import logging
import datetime

from sqlalchemy import event
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import types
from sqlalchemy import Index
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import backref, relation
from sqlalchemy.exc import InvalidRequestError

from ckan import model
from ckan.model.meta import metadata, mapper, Session
from ckan.model.types import make_uuid
from ckan.model.domain_object import DomainObject
from ckan.model.package import Package


UPDATE_FREQUENCIES = ['MANUAL', 'MONTHLY', 'WEEKLY', 'BIWEEKLY', 'DAILY', 'ALWAYS']

log = logging.getLogger(__name__)

__all__ = [
    'HarvestSource', 'harvest_source_table',
    'HarvestJob', 'harvest_job_table',
    'HarvestObject', 'harvest_object_table',
    'HarvestGatherError', 'harvest_gather_error_table',
    'HarvestObjectError', 'harvest_object_error_table',
    'HarvestLog', 'harvest_log_table'
]


harvest_source_table = None
harvest_job_table = None
harvest_object_table = None
harvest_gather_error_table = None
harvest_object_error_table = None
harvest_object_extra_table = None
harvest_log_table = None


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
        harvest_log_table.create()

        log.debug('Harvest tables created')
    else:
        from ckan.model.meta import engine
        log.debug('Harvest tables already exist')
        # Check if existing tables need to be updated
        inspector = Inspector.from_engine(engine)

        # Check if harvest_log table exist - needed for existing users
        if 'harvest_log' not in inspector.get_table_names():
            harvest_log_table.create()

        # Check if harvest_object has a index
        index_names = [index['name'] for index in inspector.get_indexes("harvest_object")]
        if "harvest_job_id_idx" not in index_names:
            log.debug('Creating index for harvest_object')
            Index("harvest_job_id_idx", harvest_object_table.c.harvest_job_id).create()
        
        if "harvest_source_id_idx" not in index_names:
            log.debug('Creating index for harvest source')
            Index("harvest_source_id_idx", harvest_object_table.c.harvest_source_id).create()
        
        if "package_id_idx" not in index_names:
            log.debug('Creating index for package')
            Index("package_id_idx", harvest_object_table.c.package_id).create()
        
        if "guid_idx" not in index_names:
            log.debug('Creating index for guid')
            Index("guid_idx", harvest_object_table.c.guid).create()
        
        index_names = [index['name'] for index in inspector.get_indexes("harvest_object_extra")]
        if "harvest_object_id_idx" not in index_names:
            log.debug('Creating index for harvest_object_extra')
            Index("harvest_object_id_idx", harvest_object_extra_table.c.harvest_object_id).create()


class HarvestError(Exception):
    pass


class HarvestDomainObject(DomainObject):
    '''Convenience methods for searching objects
    '''
    key_attr = 'id'

    @classmethod
    def get(cls, key, default=None, attr=None):
        '''Finds a single entity in the register.'''
        if attr is None:
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
    
    def get_jobs(self, status=None):
        """ get the running jobs for this source """
        
        query = Session.query(HarvestJob).filter(HarvestJob.source_id == self.id)

        if status is not None:
            query = query.filter(HarvestJob.status == status)

        return query.all()


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
    
    def get_last_finished_object(self):
        ''' Determine the last finished object in this job
            Helpful to know if a job is running or not and 
              to avoid timeouts when the source is running
        '''
        
        query = Session.query(HarvestObject)\
                    .filter(HarvestObject.harvest_job_id == self.id)\
                    .filter(HarvestObject.state == "COMPLETE")\
                    .filter(HarvestObject.import_finished.isnot(None))\
                    .order_by(HarvestObject.import_finished.desc())\
                    .first()
        
        return query
    
    def get_last_action_time(self):
        last_object = self.get_last_finished_object()
        if last_object is not None:
            return last_object.import_finished
        if self.gather_finished is not None:
            return self.gather_finished
        return self.created

    def get_gather_errors(self):
        query = Session.query(HarvestGatherError)\
                    .filter(HarvestGatherError.harvest_job_id == self.id)\
                    .order_by(HarvestGatherError.created.desc())
        
        return query.all()


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
    @classmethod
    def create(cls, message, job):
        '''
        Helper function to create an error object and save it.
        '''
        err = cls(message=message, job=job)
        try:
            err.save()
        except InvalidRequestError:
            Session.rollback()
            err.save()
        finally:
            # No need to alert administrator so don't log as an error
            log.info(message)


class HarvestObjectError(HarvestDomainObject):
    '''Object errors are raised during the **fetch** or **import** stage of a
       harvesting job, and are referenced to a specific harvest object.
    '''
    @classmethod
    def create(cls, message, object, stage=u'Fetch', line=None):
        '''
        Helper function to create an error object and save it.
        '''
        err = cls(message=message, object=object,
                  stage=stage, line=line)
        try:
            err.save()
        except InvalidRequestError:
            # Clear any in-progress sqlalchemy transactions
            try:
                Session.rollback()
            except Exception:
                pass
            try:
                Session.remove()
            except Exception:
                pass
            err.save()
        finally:
            log_message = '{0}, line {1}'.format(message, line) \
                          if line else message
            log.debug(log_message)


class HarvestLog(HarvestDomainObject):
    '''HarvestLog objects are created each time something is logged
       using python's standard logging module
    '''
    pass


def harvest_object_before_insert_listener(mapper, connection, target):
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
    global harvest_log_table

    harvest_source_table = Table(
        'harvest_source',
        metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('url', types.UnicodeText, nullable=False),
        Column('title', types.UnicodeText, default=u''),
        Column('description', types.UnicodeText, default=u''),
        Column('config', types.UnicodeText, default=u''),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
        Column('type', types.UnicodeText, nullable=False),
        Column('active', types.Boolean, default=True),
        Column('user_id', types.UnicodeText, default=u''),
        Column('publisher_id', types.UnicodeText, default=u''),
        Column('frequency', types.UnicodeText, default=u'MANUAL'),
        Column('next_run', types.DateTime),
    )
    # Was harvesting_job
    harvest_job_table = Table(
        'harvest_job',
        metadata,
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
    harvest_object_table = Table(
        'harvest_object',
        metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        # The guid is the 'identity' of the dataset, according to the source.
        # So if you reharvest it, then the harvester knows which dataset to
        # update because of this identity. The identity needs to be unique
        # within this CKAN.
        Column('guid', types.UnicodeText, default=u''),
        # When you harvest a dataset multiple times, only the latest
        # successfully imported harvest_object should be flagged 'current'.
        # The import_stage usually reads and writes it.
        Column('current', types.Boolean, default=False),
        Column('gathered', types.DateTime, default=datetime.datetime.utcnow),
        Column('fetch_started', types.DateTime),
        Column('content', types.UnicodeText, nullable=True),
        Column('fetch_finished', types.DateTime),
        Column('import_started', types.DateTime),
        Column('import_finished', types.DateTime),
        # state: WAITING, FETCH, IMPORT, COMPLETE, ERROR
        Column('state', types.UnicodeText, default=u'WAITING'),
        Column('metadata_modified_date', types.DateTime),
        Column('retry_times', types.Integer, default=0),
        Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
        Column('harvest_source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
        Column('package_id', types.UnicodeText, ForeignKey('package.id', deferrable=True),
               nullable=True),
        # report_status: 'added', 'updated', 'not modified', 'deleted', 'errored'
        Column('report_status', types.UnicodeText, nullable=True),
        Index('harvest_job_id_idx', 'harvest_job_id'),
        Index('harvest_source_id_idx', 'harvest_source_id'),
        Index('package_id_idx', 'package_id'),
        Index('guid_idx', 'guid'),
    )

    # New table
    harvest_object_extra_table = Table(
        'harvest_object_extra',
        metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('harvest_object_id', types.UnicodeText, ForeignKey('harvest_object.id')),
        Column('key', types.UnicodeText),
        Column('value', types.UnicodeText),
        Index('harvest_object_id_idx', 'harvest_object_id'),
    )

    # New table
    harvest_gather_error_table = Table(
        'harvest_gather_error',
        metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
        Column('message', types.UnicodeText),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
    )
    # New table
    harvest_object_error_table = Table(
        'harvest_object_error',
        metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('harvest_object_id', types.UnicodeText, ForeignKey('harvest_object.id')),
        Column('message', types.UnicodeText),
        Column('stage', types.UnicodeText),
        Column('line', types.Integer),
        Column('created', types.DateTime, default=datetime.datetime.utcnow),
    )
    # Harvest Log table
    harvest_log_table = Table(
        'harvest_log',
        metadata,
        Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
        Column('content', types.UnicodeText, nullable=False),
        Column('level', types.Enum('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', name='log_level')),
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
            'package': relation(
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
            'job': relation(
                HarvestJob,
                backref='gather_errors'
            ),
        },
    )

    mapper(
        HarvestObjectError,
        harvest_object_error_table,
        properties={
            'object': relation(
                HarvestObject,
                backref=backref('errors', cascade='all,delete-orphan')
            ),
        },
    )

    mapper(
        HarvestObjectExtra,
        harvest_object_extra_table,
        properties={
            'object': relation(
                HarvestObject,
                backref=backref('extras', cascade='all,delete-orphan')
            ),
        },
    )

    mapper(
        HarvestLog,
        harvest_log_table,
    )

    event.listen(HarvestObject, 'before_insert', harvest_object_before_insert_listener)


class PackageIdHarvestSourceIdMismatch(Exception):
    """
    The package created for the harvest source must match the id of the
    harvest source
    """
    pass


def clean_harvest_log(condition):
    Session.query(HarvestLog).filter(HarvestLog.created <= condition) \
        .delete(synchronize_session=False)
    try:
        Session.commit()
    except InvalidRequestError:
        Session.rollback()
        log.error('An error occurred while trying to clean-up the harvest log table')

    log.info('Harvest log table clean-up finished successfully')

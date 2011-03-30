import logging
import datetime

from ckan.model.meta import *
from ckan.model.types import make_uuid
from ckan.model.types import JsonType
from ckan.model.core import *
from ckan.model.domain_object import DomainObject
from ckan.model.package import Package

from sqlalchemy.orm import backref, relation
log = logging.getLogger(__name__)

__all__ = [
    'HarvestSource', 'harvest_source_table',
    'HarvestJob', 'harvest_job_table',
    'HarvestObject', 'harvest_object_table',
    'HarvestGatherError', 'harvest_gather_error_table',
    'HarvestObjectError', 'harvest_object_error_table', 
]

class HarvestError(Exception):
    pass

class HarvestDomainObject(DomainObject):
    '''Convenience methods for searching objects
    '''
    key_attr = 'id'

    @classmethod
    def get(self, key, default=Exception, attr=None):
        '''Finds a single entity in the register.'''
        if attr == None:
            attr = self.key_attr
        kwds = {attr: key}
        o = self.filter(**kwds).first()
        if o:
            return o
        if default != Exception:
            return default
        else:
            raise Exception('%s not found: %s' % (self.__name__, key))

    @classmethod
    def filter(self, **kwds):
        query = Session.query(self).autoflush(False)
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
    pass

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


harvest_source_table = Table('harvest_source', metadata,
    Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
    Column('url', types.UnicodeText, unique=True, nullable=False),
    Column('description', types.UnicodeText, default=u''),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    # New ones:
    Column('type',types.UnicodeText,nullable=False),
    Column('status',types.UnicodeText,nullable=False),
    # Not sure about these ones:
    Column('user_ref', types.UnicodeText, default=u''),
    Column('publisher_ref', types.UnicodeText, default=u''),
)
# Was harvesting_job
harvest_job_table = Table('harvest_job', metadata,
    Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
    Column('status', types.UnicodeText, default=u'New', nullable=False),
    # Not sure about these ones:
    Column('user_ref', types.UnicodeText, nullable=False),
)
# Was harvested_document
harvest_object_table = Table('harvest_object', metadata,
    Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
    Column('guid', types.UnicodeText, default=''),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('content', types.UnicodeText, nullable=False),
    Column('source_id', types.UnicodeText, ForeignKey('harvest_source.id')),
    # New ones:
    Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
    Column('fetch_started', DateTime),
    Column('fetch_finished', DateTime),
    Column('retry_times',types.Integer),
    # Not sure about this one. Will we always create packages from harvest objects?
    Column('package_id', types.UnicodeText, ForeignKey('package.id')),
)
# New table
harvest_gather_error_table = Table('harvest_gather_error',metadata,
    Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
    Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
    Column('error', types.UnicodeText),
)
# New table
harvest_object_error_table = Table('harvest_object_error',metadata,
    Column('id', types.UnicodeText, primary_key=True, default=make_uuid),
    Column('harvest_job_id', types.UnicodeText, ForeignKey('harvest_job.id')),
    Column('harvest_object_id', types.UnicodeText, ForeignKey('harvest_object.id')),
    Column('stage', types.UnicodeText),
)

# harvest_objects (harvested_documents) are no longer revisioned, new objects 
# are created everytime they are fetched
#vdm.sqlalchemy.make_table_stateful(harvested_document_table)
#harvested_document_revision_table = vdm.sqlalchemy.make_revisioned_table(harvested_document_table)

mapper(
    HarvestSource, 
    harvest_source_table,
    properties={ 
        'objects': relation(
            HarvestObject,
            backref=u'source',
        ),
        'jobs': relation(
            HarvestJob,
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
    # Not sure about this one
    properties={
        'package':relation(
            Package,
            # Using the plural but there should only ever be one
            backref='harvest_objects',
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
        'job':relation(
            HarvestJob,
            backref='gather_errors'
        ),
        'object':relation(
            HarvestObject,
            backref='errors'
        ),
    },
)
#vdm.sqlalchemy.modify_base_object_mapper(HarvestedDocument, Revision, State)
#HarvestedDocumentRevision = vdm.sqlalchemy.create_object_version(
#                mapper, HarvestedDocument, harvested_document_revision_table)

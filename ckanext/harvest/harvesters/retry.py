import logging
import re

from sqlalchemy.sql import update,and_, bindparam

from ckan import model
from ckan.model import Session, Package

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestObjectError

log = logging.getLogger(__name__)


'''
There should really be a separate table for retries. Now errors are used for
storing retry information. On the positive side, if there is a way to view the
objects and the associated errors then the retry will show up there. If that
is of any use to anyone.
'''

class HarvesterRetry(object):
    '''
    Class for dealing with harvest_objects that need to be retried later.
    '''

    @staticmethod
    def _retry_message(harvest_job):
        return u'retry ' + harvest_job.source_id

    @staticmethod
    def mark_for_retry(harvest_object):
        '''
        Marks a harvest object for retry later.
        '''
        msg = HarvesterRetry._retry_message(harvest_object.harvest_job)
        err = HarvestObjectError(message=msg, object=harvest_object, stage=u'')
        err.save()

    def find_all_retries(self, harvest_job):
        '''
        Finds list of all retries related to the earlier harvest_jobs that
        match the given harvest_job. Returns a list of HarvestObjects.
        '''
        msg = HarvesterRetry._retry_message(harvest_job)
        self._objs = Session.query(HarvestObjectError).filter(
            HarvestObjectError.message==msg).all()
        harvest_objects = []
        for obj in self._objs:
            harvest_objects.append(obj.harvest_object)
        return harvest_objects

    def clear_retry_marks(self):
        '''
        Finds all retries related to previous harvest_job and clears the marks.
        Only do this once you have successfully built a list of harvest_objects.
        '''
        # Not calling find_all_retries before this is really a bug.
        assert hasattr(self, '_objs')
        for obj in self._objs:
            obj.harvest_object.retry_times = obj.harvest_object.retry_times + 1
            obj.harvest_object.save()
            obj.delete()
        del self._objs


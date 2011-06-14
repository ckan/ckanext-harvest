import urllib2

from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckanclient import CkanClient

import logging
log = logging.getLogger(__name__)

from base import HarvesterBase

class CKANHarvester(HarvesterBase):
    '''
    A Harvester for CKAN instances
    '''
    config = None

    #TODO: check different API versions
    api_version = '2'

    def _get_rest_api_offset(self):
        return '/api/%s/rest' % self.api_version

    def _get_search_api_offset(self):
        return '/api/%s/search' % self.api_version

    def _get_content(self, url):
        http_request = urllib2.Request(
            url = url,
        )

        try:
            http_response = urllib2.urlopen(http_request)

            return http_response.read()
        except Exception, e:
            raise e

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': 'ckan',
            'title': 'CKAN',
            'description': 'Harvests remote CKAN instances',
            'form_config_interface':'Text'
        }

    def validate_config(self,config):
        try:
            config_obj = json.loads(config)
        except ValueError,e:
            raise e

        return config


    def gather_stage(self,harvest_job):
        log.debug('In CKANHarvester gather_stage (%s)' % harvest_job.source.url)
        get_all_packages = True
        package_ids = []

        if not self.config:
            self._set_config(harvest_job.source.config)

        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
                        .filter(HarvestJob.source==harvest_job.source) \
                        .filter(HarvestJob.gather_finished!=None) \
                        .filter(HarvestJob.id!=harvest_job.id) \
                        .order_by(HarvestJob.gather_finished.desc()) \
                        .limit(1).first()

        # Get source URL
        base_url = harvest_job.source.url.rstrip('/')
        base_rest_url = base_url + self._get_rest_api_offset()
        base_search_url = base_url + self._get_search_api_offset()

        if previous_job and not previous_job.gather_errors and not len(previous_job.objects) == 0:
            get_all_packages = False

            # Request only the packages modified since last harvest job
            last_time = harvest_job.gather_started.isoformat()
            url = base_search_url + '/revision?since_time=%s' % last_time

            try:
                content = self._get_content(url)

                revision_ids = json.loads(content)
                if len(revision_ids):
                    for revision_id in revision_ids:
                        url = base_rest_url + '/revision/%s' % revision_id
                        try:
                            content = self._get_content(url)
                        except Exception,e:
                            self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
                            continue

                        revision = json.loads(content)
                        for package_id in revision.packages:
                            if not package_id in package_ids:
                                package_ids.append(package_id)
                else:
                    log.info('No packages have been updated on the remote CKAN instance since the last harvest job')
                    return None

            except urllib2.HTTPError,e:
                if e.getcode() == 400:
                    log.info('CKAN instance %s does not suport revision filtering' % base_url)
                    get_all_packages = True
                else:
                    self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
                    return None



        if get_all_packages:
            # Request all remote packages
            url = base_rest_url + '/package'
            try:
                content = self._get_content(url)
            except Exception,e:
                self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
                return None

            package_ids = json.loads(content)

        try:
            object_ids = []
            if len(package_ids):
                for package_id in package_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = package_id, job = harvest_job)
                    obj.save()
                    object_ids.append(obj.id)

                return object_ids

            else:
               self._save_gather_error('No packages received for URL: %s' % url,
                       harvest_job)
               return None
        except Exception, e:
            self._save_gather_error('%r'%e.message,harvest_job)


    def fetch_stage(self,harvest_object):
        log.debug('In CKANHarvester fetch_stage')

        if not self.config:
            self._set_config(harvest_object.job.source.config)

        # Get source URL
        url = harvest_object.source.url.rstrip('/')
        url = url + self._get_rest_api_offset() + '/package/' + harvest_object.guid

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_object_error('Unable to get content for package: %s: %r' % \
                                        (url, e),harvest_object)
            return None

        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
        harvest_object.save()
        return True

    def import_stage(self,harvest_object):
        log.debug('In CKANHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,
                    harvest_object, 'Import')
            return False

        if not self.config:
           self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)
            return self._create_or_update_package(package_dict,harvest_object)
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')


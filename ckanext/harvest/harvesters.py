import urllib2

from ckan.logic.action.create import package_create_rest
from ckan.logic.action.update import package_update_rest
from ckan.logic.action.get import package_show
from ckan.logic.schema import default_package_schema
from ckan.logic import ValidationError,NotFound
from ckan import model
from ckan.model import Session
from ckan.lib.navl.validators import ignore_missing

from ckan.lib.helpers import json

from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

import logging
log = logging.getLogger(__name__)

class MockTranslator(object):
    def ugettext(self, value):
        return value

    def ungettext(self, singular, plural, n):
        if n > 1:
            return plural
        return singular

class CKANHarvester(SingletonPlugin):
    '''
    A Harvester for CKAN instances
    '''

    implements(IHarvester)

    #TODO: check different API versions
    api_version = '2'


    def __init__(self,**kw):
        from paste.registry import Registry
        import pylons
        self.registry=Registry()
        self.registry.prepare()

        self.translator_obj=MockTranslator()
        self.registry.register(pylons.translator, self.translator_obj)


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

    def _save_gather_error(self,message,job):
        err = HarvestGatherError(message=message,job=job)
        err.save()
        log.error(message)

    def _save_object_error(self,message,obj,stage=u'Fetch'):
        err = HarvestObjectError(message=message,object=obj,stage=stage)
        err.save()
        log.error(message)

    def info(self):
        return {
            'name': 'ckan',
            'title': 'CKAN',
            'description': 'Harvests remote CKAN instances'
        }

    def gather_stage(self,harvest_job):
        log.debug('In CKANHarvester gather_stage (%s)' % harvest_job.source.url)

        get_all_packages = True
        package_ids = []

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
        
        if previous_job and not previous_job.gather_errors:
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
               self._save_gather_error('No packages received for URL: %s' % url,harvest_job)
               return None
        except Exception, e:
            self._save_gather_error('%r'%e.message,harvest_job)


    def fetch_stage(self,harvest_object):
        log.debug('In CKANHarvester fetch_stage')
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
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:

            # harvest_object.content is the result of a package REST API call
            package_dict = json.loads(harvest_object.content)

            # Save metadata modified date in Harvest Object
            if not 'metadata_modified' in package_dict:
                # Get the date from the revision
                url = harvest_object.job.source.url.rstrip('/')
                url = url + self._get_rest_api_offset() + '/revision/%s' % package_dict['revision_id']

                try:
                    content = self._get_content(url)
                    revision_dict = json.loads(content)
                    package_dict['metadata_modified'] = revision_dict['timestamp']
                except Exception,e:
                    self._save_gather_error('Unable to get revision %s info : %r' % \
                                                (package_dict['revision_id'], e),harvest_job)

            harvest_object.metadata_modified_date = package_dict['metadata_modified']
            harvest_object.save()

            ## change default schema
            schema = default_package_schema()
            schema["id"] = [ignore_missing, unicode]

            context = {
                'model': model,
                'session':Session,
                'user': u'harvest',
                'api_version':'2',
                'schema': schema,
            }

            # Ugly Hack: tags in DGU are created with Upper case and spaces,
            # and the validator does not like them
            if 'tags' in package_dict:
                new_tags = []
                for tag in package_dict['tags']:
                    new_tags.append(tag.lower().replace(' ','_'))
                package_dict['tags'] = new_tags

            # Check if package exists
            context.update({'id':package_dict['id']})
            try:
                existing_package_dict = package_show(context)
                # Check modified date
                if package_dict['metadata_modified'] > existing_package_dict['metadata_modified']:
                    log.info('Package with GUID %s exists and needs to be updated' % harvest_object.guid)
                    # Update package
                    updated_package = package_update_rest(package_dict,context)

                    harvest_object.package_id = updated_package['id']
                    harvest_object.save()
                else:
                    log.info('Package with GUID %s not updated, skipping...' % harvest_object.guid)

            except NotFound:
                # Package needs to be created
                #del package_dict['id']
                del context['id']
                log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
                new_package = package_create_rest(package_dict,context)
                harvest_object.package_id = new_package['id']
                harvest_object.save()

            return True
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r'%(harvest_object.guid,e.error_dict),harvest_object,'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')


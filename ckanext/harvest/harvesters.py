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
from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
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

    def __init__(self,**kw):
        from paste.registry import Registry
        import pylons
        self.registry=Registry() 
        self.registry.prepare() 

        self.translator_obj=MockTranslator()
        self.registry.register(pylons.translator, self.translator_obj)


    def _get_api_offset(self):
        #TODO: check different API versions?
        return '/api/2/rest'

    def _get_content(self, url):
        #TODO: configure
        http_request = urllib2.Request(
            url = url,
            headers = {'Authorization' : 'fcff821f-1f92-42ef-8c52-7c38d74a7291'}
        )

        try:
            #http_response = urllib2.urlopen(url)
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

    def get_type(self):
        return 'CKAN'

    def gather_stage(self,harvest_job):
        log.debug('In CKANHarvester gather_stage')

        # Get source URL
        url = harvest_job.source.url.rstrip('/')
        url = url + self._get_api_offset() + '/package'

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (url, e),harvest_job)
            return None

        try:
            package_ids = json.loads(content)
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
        url = url + self._get_api_offset() + '/package/' + harvest_object.guid

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

            # harvest_object.content is the result of an API call like
            # http://ec2-46-51-149-132.eu-west-1.compute.amazonaws.com:8081/api/2/rest/package/77d93608-3a3e-42e5-baab-3521afb504f1
            package_dict = json.loads(harvest_object.content)

            # Save reference date in Harvest Object
            harvest_object.reference_date = package_dict['metadata_modified']
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


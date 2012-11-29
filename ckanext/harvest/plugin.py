from logging import getLogger

from pylons import config

from ckan import logic
import ckan.plugins as p
from ckan.lib.plugins import DefaultDatasetForm
from ckan.lib.navl import dictization_functions

from ckanext.harvest.model import setup as model_setup
from ckanext.harvest.model import UPDATE_FREQUENCIES



log = getLogger(__name__)
assert not log.disabled

DATASET_TYPE_NAME = 'harvest_source'

class Harvest(p.SingletonPlugin, DefaultDatasetForm):

    p.implements(p.IConfigurable)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IDatasetForm)

    ## IDatasetForm

    def is_fallback(self):
        return False

    def package_types(self):
        return [DATASET_TYPE_NAME]

    def package_form(self):
        return 'source/new_source_form.html'

    def setup_template_variables(self, context, data_dict):
        harvesters_info = logic.get_action('harvesters_info_show')(context,{})

        p.toolkit.c.frequencies = [
                {'text': p.toolkit._(f.title()), 'value': f}
                for f in UPDATE_FREQUENCIES
                ]
        p.toolkit.c.harvester_types = [
                {'text': p.toolkit._(h['title']), 'value': h['name']}
                for h in harvesters_info
                ]
        p.toolkit.c.harvesters_info = harvesters_info

    def form_to_db_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvest.logic.schema import harvest_source_form_schema

        return harvest_source_form_schema()

    def check_data_dict(self, data_dict, schema=None):
        '''Check if the return data is correct, mostly for checking out
        if spammers are submitting only part of the form'''

        surplus_keys_schema = ['__extras', '__junk', 'extras_validation', 'save',
                               'return_to', 'type', 'state']

        #TODO: state and delete

        if not schema:
            schema = self.form_to_db_schema()
        schema_keys = schema.keys()
        keys_in_schema = set(schema_keys) - set(surplus_keys_schema)

        missing_keys = keys_in_schema - set(data_dict.keys())
        if missing_keys:
            log.info('incorrect form fields posted, missing %s' % missing_keys)
            raise dictization_functions.DataError(data_dict)

    def configure(self, config):

        auth_profile = config.get('ckan.harvest.auth.profile',None)

        if auth_profile:
            # Check if auth profile exists
            module_root = 'ckanext.harvest.logic.auth'
            module_path = '%s.%s' % (module_root, auth_profile)
            try:
                module = __import__(module_path)
            except ImportError,e:
                raise ImportError('Unknown auth profile: %s' % auth_profile)

            # If we are using the publisher auth profile, make sure CKAN core
            # also uses it.
            if auth_profile == 'publisher' and \
                not config.get('ckan.auth.profile','') == 'publisher':
                raise Exception('You must enable the "publisher" auth profile'
                      +' in CKAN in order to use it on the harvest extension'
                      +' (adding "ckan.auth.profile=publisher" to your ini file)')

        # Setup harvest model
        model_setup()

    def before_map(self, map):

        controller = 'ckanext.harvest.controllers.view:ViewController'
        map.connect('harvest', '/harvest',controller=controller,action='index')

        map.connect('/harvest/new', controller=controller, action='new')
        map.connect('/harvest/edit/:id', controller=controller, action='edit')
        map.connect('/harvest/delete/:id',controller=controller, action='delete')
        map.connect('/harvest/:id', controller=controller, action='read')

        map.connect('harvesting_job_create', '/harvest/refresh/:id',controller=controller,
                action='create_harvesting_job')

        map.connect('/harvest/object/:id', controller=controller, action='show_object')

        return map

    def update_config(self, config):
        # check if new templates
        templates = 'templates'
        if p.toolkit.check_ckan_version(min_version='2.0'):
            if not p.toolkit.asbool(config.get('ckan.legacy_templates', False)):
                templates = 'templates_new'
        p.toolkit.add_template_directory(config, templates)
        p.toolkit.add_public_directory(config, 'public')

    def get_actions(self):
        from ckanext.harvest.logic.action.get import (harvest_source_show,
                                                      harvest_source_list,
                                                      harvest_source_for_a_dataset,
                                                      harvest_job_show,
                                                      harvest_job_list,
                                                      harvest_object_show,
                                                      harvest_object_list,
                                                      harvesters_info_show,)
        from ckanext.harvest.logic.action.create import (harvest_source_create,
                                                         harvest_job_create,
                                                         harvest_job_create_all,)
        from ckanext.harvest.logic.action.update import (harvest_source_update,
                                                         harvest_objects_import,
                                                         harvest_jobs_run)
        from ckanext.harvest.logic.action.delete import (harvest_source_delete,)

        return {
            'harvest_source_show': harvest_source_show,
            'harvest_source_list': harvest_source_list,
            'harvest_source_for_a_dataset': harvest_source_for_a_dataset,
            'harvest_job_show': harvest_job_show,
            'harvest_job_list': harvest_job_list,
            'harvest_object_show': harvest_object_show,
            'harvest_object_list': harvest_object_list,
            'harvesters_info_show': harvesters_info_show,
            'harvest_source_create': harvest_source_create,
            'harvest_job_create': harvest_job_create,
            'harvest_job_create_all': harvest_job_create_all,
            'harvest_source_update': harvest_source_update,
            'harvest_source_delete': harvest_source_delete,
            'harvest_objects_import': harvest_objects_import,
            'harvest_jobs_run':harvest_jobs_run
        }

    def get_auth_functions(self):

        module_root = 'ckanext.harvest.logic.auth'
        auth_profile = config.get('ckan.harvest.auth.profile', '')

        auth_functions = _get_auth_functions(module_root)
        if auth_profile:
            module_root = '%s.%s' % (module_root, auth_profile)
            auth_functions = _get_auth_functions(module_root,auth_functions)

        log.debug('Using auth profile at %s' % module_root)

        return auth_functions

def _get_auth_functions(module_root, auth_functions = {}):

    for auth_module_name in ['get', 'create', 'update','delete']:
        module_path = '%s.%s' % (module_root, auth_module_name,)
        try:
            module = __import__(module_path)
        except ImportError,e:
            log.debug('No auth module for action "%s"' % auth_module_name)
            continue

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_'):
                auth_functions[key] = value


    return auth_functions


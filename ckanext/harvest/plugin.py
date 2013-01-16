import types
from logging import getLogger

from ckan import logic
from ckan import model
import ckan.plugins as p
from ckan.lib.plugins import DefaultDatasetForm
from ckan.lib.navl import dictization_functions

from ckanext.harvest import logic as harvest_logic

from ckanext.harvest.model import setup as model_setup
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject


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
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)

    startup = False

    ## IPackageController

    def after_create(self, context, data_dict):
        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME and not self.startup:
            # Create an actual HarvestSource object
            _create_harvest_source_object(data_dict)

    def after_update(self, context, data_dict):
        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # Edit the actual HarvestSource object
            _update_harvest_source_object(data_dict)

    def after_show(self, context, data_dict):

        def add_extra(data_dict, key, value):
            if not 'extras' in data_dict:
                data_dict['extras'] = []

            data_dict['extras'].append({
                'key': key, 'value': '"{0}"'.format(value), 'state': u'active'
            })

        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # This is a harvest source dataset, add extra info from the
            # HarvestSource object
            source = HarvestSource.get(data_dict['id'])
            if not source:
                log.error('Harvest source not found for dataset {0}'.format(data_dict['id']))
                return data_dict

            data_dict['status'] = harvest_logic.action.get.harvest_source_show_status(context, {'id': source.id})

        elif not 'type' in data_dict or data_dict['type'] != DATASET_TYPE_NAME:
            # This is a normal dataset, check if it was harvested and if so, add
            # info about the HarvestObject and HarvestSource

            harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id==data_dict['id']) \
                    .filter(HarvestObject.current==True) \
                    .first()

            if harvest_object:
                for key, value in [
                    ('harvest_object_id', harvest_object.id),
                    ('harvest_source_id', harvest_object.source.id),
                    ('harvest_source_title', harvest_object.source.title),
                        ]:
                    add_extra(data_dict, key, value)

        return data_dict

    ## IDatasetForm

    def is_fallback(self):
        return False

    def package_types(self):
        return [DATASET_TYPE_NAME]

    def package_form(self):
        return 'source/new_source_form.html'

    def search_template(self):
        return 'source/search.html'

    def read_template(self):
        return 'source/read.html'

    def new_template(self):
        return 'source/new.html'

    def edit_template(self):
        return 'source/edit.html'

    def setup_template_variables(self, context, data_dict):

        p.toolkit.c.harvest_source = p.toolkit.c.pkg

        p.toolkit.c.dataset_type = DATASET_TYPE_NAME

    def form_to_db_schema_options(self, options):
        '''
            Similar to form_to_db_schema but with further options to allow
            slightly different schemas, eg for creation or deletion on the API.
        '''
        schema = self.form_to_db_schema()

        # Tweak the default schema to allow using the same id as the harvest source
        # if creating datasets for the harvest sources
        if self.startup:
            schema['id'] = [unicode]
        return schema

    def form_to_db_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvest.logic.schema import harvest_source_form_to_db_schema

        return harvest_source_form_to_db_schema()

    def db_to_form_schema_options(self, options):
        '''
            Similar to db_to_form_schema but with further options to allow
            slightly different schemas, eg for creation or deletion on the API.
        '''
        if options.get('type') == 'show':
            return None
        else:
            return self.db_to_form_schema()
        

    def db_to_form_schema(self):
        '''
        Returns the schema for mapping package data from the database into a
        format suitable for the form
        '''
        from ckanext.harvest.logic.schema import harvest_source_db_to_form_schema

        return harvest_source_db_to_form_schema()

    def check_data_dict(self, data_dict, schema=None):
        '''Check if the return data is correct, mostly for checking out
        if spammers are submitting only part of the form'''

        surplus_keys_schema = ['__extras', '__junk', 'extras',
                               'extras_validation', 'save', 'return_to', 'type',
                               'state']

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

        self.startup = True

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

        self.startup = False

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

    ## IActions

    def get_actions(self):

        module_root = 'ckanext.harvest.logic.action'
        action_functions = _get_logic_functions(module_root)

        return action_functions

    ## IAuthFunctions

    def get_auth_functions(self):

        module_root = 'ckanext.harvest.logic.auth'
        auth_functions = _get_logic_functions(module_root)

        return auth_functions

    ## ITemplateHelpers

    def get_helpers(self):
        from ckanext.harvest import helpers as harvest_helpers
        return {
                'package_list_for_source': harvest_helpers.package_list_for_source,
                'harvesters_info': harvest_helpers.harvesters_info,
                'harvester_types': harvest_helpers.harvester_types,
                'harvest_frequencies': harvest_helpers.harvest_frequencies,
                }


def _get_logic_functions(module_root, logic_functions = {}):

    for module_name in ['get', 'create', 'update','delete']:
        module_path = '%s.%s' % (module_root, module_name,)
        try:
            module = __import__(module_path)
        except ImportError:
            log.debug('No auth module for action "{0}"'.format(module_name))
            continue

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_') and isinstance(value, types.FunctionType):
                logic_functions[key] = value

    return logic_functions

def _create_harvest_source_object(data_dict):
    '''
        Creates an actual HarvestSource object with the data dict
        of the harvest_source dataset. All validation and authorization
        checks should be used by now, so this function is not to be used
        directly to create harvest sources. The created harvest source will
        have the same id as the dataset.

        :param data_dict: A standard package data_dict

        :returns: The created HarvestSource object
        :rtype: HarvestSource object
    '''

    log.info('Creating harvest source: %r', data_dict)

    source = HarvestSource()

    source.id = data_dict['id']
    source.url = data_dict['url'].strip()

    # Avoids clashes with the dataset type
    source.type = data_dict['source_type']

    opt = ['active', 'title', 'description', 'user_id',
           'publisher_id', 'config', 'frequency']
    for o in opt:
        if o in data_dict and data_dict[o] is not None:
            source.__setattr__(o,data_dict[o])

    #TODO: state / deleted
    if 'active' in data_dict:
        source.active = data_dict['active']

    # Don't commit yet, let package_create do it
    source.add()
    log.info('Harvest source created: %s', source.id)

    return source

def _update_harvest_source_object(data_dict):
    '''
        Updates an actual HarvestSource object with the data dict
        of the harvest_source dataset. All validation and authorization
        checks should be used by now, so this function is not to be used
        directly to update harvest sources.

        :param data_dict: A standard package data_dict

        :returns: The created HarvestSource object
        :rtype: HarvestSource object
    '''

    source_id = data_dict.get('id')

    log.info('Harvest source %s update: %r', source_id, data_dict)
    source = HarvestSource.get(source_id)
    if not source:
        log.error('Harvest source %s does not exist', source_id)
        raise logic.NotFound('Harvest source %s does not exist' % source_id)


    fields = ['url', 'title', 'description', 'user_id',
              'publisher_id', 'frequency']
    for f in fields:
        if f in data_dict and data_dict[f] is not None:
            if f == 'url':
                data_dict[f] = data_dict[f].strip()
            source.__setattr__(f,data_dict[f])

    # Avoids clashes with the dataset type
    if 'source_type' in data_dict:
        source.type = data_dict['source_type']

    if 'active' in data_dict:
        source.active = data_dict['active']

    if 'config' in data_dict:
        source.config = data_dict['config']

    # Don't commit yet, let package_create do it
    source.add()

    # Abort any pending jobs
    if not source.active:
        jobs = HarvestJob.filter(source=source,status=u'New')
        log.info('Harvest source %s not active, so aborting %i outstanding jobs', source_id, jobs.count())
        if jobs:
            for job in jobs:
                job.status = u'Aborted'
                job.add()

    return source

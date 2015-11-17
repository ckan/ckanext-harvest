import types
from logging import getLogger

from sqlalchemy.util import OrderedDict

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

DATASET_TYPE_NAME = 'harvest'

class Harvest(p.SingletonPlugin, DefaultDatasetForm):

    p.implements(p.IConfigurable)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IDatasetForm)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IFacets, inherit=True)

    startup = False

    ## IPackageController

    def after_create(self, context, data_dict):
        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME and not self.startup:
            # Create an actual HarvestSource object
            _create_harvest_source_object(context, data_dict)

    def after_update(self, context, data_dict):
        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # Edit the actual HarvestSource object
            _update_harvest_source_object(context, data_dict)

    def after_delete(self, context, data_dict):

        package_dict = p.toolkit.get_action('package_show')(context, {'id': data_dict['id']})

        if 'type' in package_dict and package_dict['type'] == DATASET_TYPE_NAME:
            # Delete the actual HarvestSource object
            _delete_harvest_source_object(context, package_dict)

    def before_view(self, data_dict):

        # check_ckan_version should be more clever than this
        if p.toolkit.check_ckan_version(max_version='2.1.99') and (
           not 'type' in data_dict or data_dict['type'] != DATASET_TYPE_NAME):
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
                    _add_extra(data_dict, key, value)
        return data_dict


    def after_show(self, context, data_dict):

        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # This is a harvest source dataset, add extra info from the
            # HarvestSource object
            source = HarvestSource.get(data_dict['id'])
            if not source:
                log.error('Harvest source not found for dataset {0}'.format(data_dict['id']))
                return data_dict

            data_dict['status'] = p.toolkit.get_action('harvest_source_show_status')(context, {'id': source.id})

        elif not 'type' in data_dict or data_dict['type'] != DATASET_TYPE_NAME:
            # This is a normal dataset, check if it was harvested and if so, add
            # info about the HarvestObject and HarvestSource

            harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id==data_dict['id']) \
                    .filter(HarvestObject.current==True) \
                    .first()

            # If the harvest extras are there, remove them. This can happen eg
            # when calling package_update or resource_update, which call
            # package_show
            if data_dict.get('extras'):
                data_dict['extras'][:] = [e for e in data_dict.get('extras', [])
                                          if not e['key']
                                          in ('harvest_object_id', 'harvest_source_id', 'harvest_source_title',)]


            # We only want to add these extras at index time so they are part
            # of the cached data_dict used to display, search results etc. We
            # don't want them added when editing the dataset, otherwise we get
            # duplicated key errors.
            # The only way to detect indexing right now is checking that
            # validate is set to False.
            if harvest_object and not context.get('validate', True):
                for key, value in [
                    ('harvest_object_id', harvest_object.id),
                    ('harvest_source_id', harvest_object.source.id),
                    ('harvest_source_title', harvest_object.source.title),
                        ]:
                    _add_extra(data_dict, key, value)

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

        p.toolkit.c.harvest_source = p.toolkit.c.pkg_dict

        p.toolkit.c.dataset_type = DATASET_TYPE_NAME


    def create_package_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvest.logic.schema import harvest_source_create_package_schema
        schema = harvest_source_create_package_schema()
        if self.startup:
            schema['id'] = [unicode]

        return schema

    def update_package_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvest.logic.schema import harvest_source_update_package_schema
        schema = harvest_source_update_package_schema()

        return schema

    def show_package_schema(self):
        '''
        Returns the schema for mapping package data from the database into a
        format suitable for the form
        '''
        from ckanext.harvest.logic.schema import harvest_source_show_package_schema

        return harvest_source_show_package_schema()

    def configure(self, config):

        self.startup = True

        # Setup harvest model
        model_setup()

        self.startup = False

    def before_map(self, map):

        # Most of the routes are defined via the IDatasetForm interface
        # (ie they are the ones for a package type)
        controller = 'ckanext.harvest.controllers.view:ViewController'

        map.connect('{0}_delete'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/delete/:id',controller=controller, action='delete')
        map.connect('{0}_refresh'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/refresh/:id',controller=controller,
                action='refresh')
        map.connect('{0}_admin'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/admin/:id', controller=controller, action='admin')
        map.connect('{0}_about'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/about/:id', controller=controller, action='about')
        map.connect('{0}_clear'.format(DATASET_TYPE_NAME), '/' + DATASET_TYPE_NAME + '/clear/:id', controller=controller, action='clear')

        map.connect('harvest_job_list', '/' + DATASET_TYPE_NAME + '/{source}/job', controller=controller, action='list_jobs')
        map.connect('harvest_job_show_last', '/' + DATASET_TYPE_NAME + '/{source}/job/last', controller=controller, action='show_last_job')
        map.connect('harvest_job_show', '/' + DATASET_TYPE_NAME + '/{source}/job/{id}', controller=controller, action='show_job')

        map.connect('harvest_object_show', '/' + DATASET_TYPE_NAME + '/object/:id', controller=controller, action='show_object')
        map.connect('harvest_object_for_dataset_show', '/dataset/harvest_object/:id', controller=controller, action='show_object', ref_type='dataset')

        org_controller = 'ckanext.harvest.controllers.organization:OrganizationController'
        map.connect('{0}_org_list'.format(DATASET_TYPE_NAME), '/organization/' + DATASET_TYPE_NAME + '/' + '{id}', controller=org_controller, action='source_list')

        return map

    def update_config(self, config):
        # check if new templates
        templates = 'templates'
        if p.toolkit.check_ckan_version(min_version='2.0'):
            if not p.toolkit.asbool(config.get('ckan.legacy_templates', False)):
                templates = 'templates_new'
        p.toolkit.add_template_directory(config, templates)
        p.toolkit.add_public_directory(config, 'public')
        p.toolkit.add_resource('fanstatic_library', 'ckanext-harvest')
        p.toolkit.add_resource('public/ckanext/harvest/javascript', 'harvest-extra-field')

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
                'link_for_harvest_object': harvest_helpers.link_for_harvest_object,
                'harvest_source_extra_fields': harvest_helpers.harvest_source_extra_fields,
                }

    def dataset_facets(self, facets_dict, package_type):

        if package_type <> 'harvest':
            return facets_dict

        return OrderedDict([('frequency', 'Frequency'),
                            ('source_type','Type'),
                           ])

    def organization_facets(self, facets_dict, organization_type, package_type):

        if package_type <> 'harvest':
            return facets_dict

        return OrderedDict([('frequency', 'Frequency'),
                            ('source_type','Type'),
                           ])

def _add_extra(data_dict, key, value):
    if not 'extras' in data_dict:
        data_dict['extras'] = []

    data_dict['extras'].append({
        'key': key, 'value': value, 'state': u'active'
    })

def _get_logic_functions(module_root, logic_functions = {}):

    for module_name in ['get', 'create', 'update', 'patch', 'delete']:
        module_path = '%s.%s' % (module_root, module_name,)

        module = __import__(module_path)

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_') and  (hasattr(value, '__call__')
                        and (value.__module__ == module_path)):
                logic_functions[key] = value

    return logic_functions

def _create_harvest_source_object(context, data_dict):
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

    source.active = not data_dict.get('state', None) == 'deleted'

    # Don't commit yet, let package_create do it
    source.add()
    log.info('Harvest source created: %s', source.id)

    return source

def _update_harvest_source_object(context, data_dict):
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

    if 'config' in data_dict:
        source.config = data_dict['config']

    # Don't change state unless explicitly set in the dict
    if 'state' in data_dict:
      source.active = data_dict.get('state') == 'active'

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

def _delete_harvest_source_object(context, data_dict):
    '''
        Deletes an actual HarvestSource object with the id provided on the
        data dict of the harvest_source dataset. Similarly to the datasets,
        the source object is not actually deleted, just flagged as inactive.
        All validation and authorization checks should be used by now, so
        this function is not to be used directly to delete harvest sources.

        :param data_dict: A standard package data_dict

        :returns: The deleted HarvestSource object
        :rtype: HarvestSource object
    '''

    source_id = data_dict.get('id')

    log.info('Deleting harvest source: %s', source_id)

    source = HarvestSource.get(source_id)
    if not source:
        log.warn('Harvest source %s does not exist', source_id)
        raise p.toolkit.ObjectNotFound('Harvest source %s does not exist' % source_id)

    # Don't actually delete the record, just flag it as inactive
    source.active = False
    source.save()

    # Abort any pending jobs
    jobs = HarvestJob.filter(source=source, status=u'New')
    if jobs:
        log.info('Aborting %i jobs due to deleted harvest source', jobs.count())
        for job in jobs:
            job.status = u'Aborted'
            job.save()

    log.debug('Harvest source %s deleted', source_id)

    return source

# -*- coding: utf-8 -*-

import os
import json
from logging import getLogger

from six import string_types, text_type
from collections import OrderedDict

from ckan import logic
from ckan import model
import ckan.plugins as p
from ckan.lib.plugins import DefaultDatasetForm

try:
    from ckan.lib.plugins import DefaultTranslation
except ImportError:
    class DefaultTranslation():
        pass

import ckanext.harvest
from ckanext.harvest.model import setup as model_setup
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvest.log import DBLogHandler

from ckanext.harvest.utils import (
    DATASET_TYPE_NAME
)

if p.toolkit.check_ckan_version(min_version='2.9.0'):
    from ckanext.harvest.plugin.flask_plugin import MixinPlugin
else:
    from ckanext.harvest.plugin.pylons_plugin import MixinPlugin

log = getLogger(__name__)
assert not log.disabled


class Harvest(MixinPlugin, p.SingletonPlugin, DefaultDatasetForm, DefaultTranslation):

    p.implements(p.IConfigurable)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IDatasetForm)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IFacets, inherit=True)
    if p.toolkit.check_ckan_version(min_version='2.5.0'):
        p.implements(p.ITranslation, inherit=True)

    startup = False

    # ITranslation
    def i18n_directory(self):
        u'''Change the directory of the .mo translation files'''
        return os.path.join(
            os.path.dirname(ckanext.harvest.__file__),
            'i18n'
        )

    # IPackageController

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
           'type' not in data_dict or data_dict['type'] != DATASET_TYPE_NAME):
            # This is a normal dataset, check if it was harvested and if so, add
            # info about the HarvestObject and HarvestSource
            harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id == data_dict['id']) \
                    .filter(HarvestObject.current==True).first() # noqa

            if harvest_object:
                for key, value in [
                    ('harvest_object_id', harvest_object.id),
                    ('harvest_source_id', harvest_object.source.id),
                    ('harvest_source_title', harvest_object.source.title),
                        ]:
                    _add_extra(data_dict, key, value)
        return data_dict

    def before_search(self, search_params):
        '''Prevents the harvesters being shown in dataset search results.'''

        fq = search_params.get('fq', '')
        if 'dataset_type:harvest' not in fq:
            fq = u"{0} -dataset_type:harvest".format(search_params.get('fq', ''))
            search_params.update({'fq': fq})

        return search_params

    def after_show(self, context, data_dict):

        if 'type' in data_dict and data_dict['type'] == DATASET_TYPE_NAME:
            # This is a harvest source dataset, add extra info from the
            # HarvestSource object
            source = HarvestSource.get(data_dict['id'])
            if not source:
                log.error('Harvest source not found for dataset {0}'.format(data_dict['id']))
                return data_dict

            st_action_name = 'harvest_source_show_status'
            try:
                status_action = p.toolkit.get_action(st_action_name)
            except KeyError:
                logic.clear_actions_cache()
                status_action = p.toolkit.get_action(st_action_name)

            data_dict['status'] = status_action(context, {'id': source.id})

        elif 'type' not in data_dict or data_dict['type'] != DATASET_TYPE_NAME:
            # This is a normal dataset, check if it was harvested and if so, add
            # info about the HarvestObject and HarvestSource

            harvest_object = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.package_id == data_dict['id']) \
                    .filter(HarvestObject.current == True).first() # noqa

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

    # IDatasetForm

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

        p.toolkit.c.dataset_type = DATASET_TYPE_NAME

    def create_package_schema(self):
        '''
        Returns the schema for mapping package data from a form to a format
        suitable for the database.
        '''
        from ckanext.harvest.logic.schema import harvest_source_create_package_schema
        schema = harvest_source_create_package_schema()
        if self.startup:
            schema['id'] = [text_type]

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

        # Configure database logger
        _configure_db_logger(config)

        self.startup = False

    def update_config(self, config):
        if not p.toolkit.check_ckan_version(min_version='2.0'):
            assert 0, 'CKAN before 2.0 not supported by ckanext-harvest - '\
                'genshi templates not supported any more'
        if p.toolkit.asbool(config.get('ckan.legacy_templates', False)):
            log.warn('Old genshi templates not supported any more by '
                     'ckanext-harvest so you should set ckan.legacy_templates '
                     'option to True any more.')
        p.toolkit.add_template_directory(config, '../templates')
        p.toolkit.add_public_directory(config, '../public')
        p.toolkit.add_resource('../fanstatic_library', 'ckanext-harvest')
        p.toolkit.add_resource('../public/ckanext/harvest/javascript', 'harvest-extra-field')

        if p.toolkit.check_ckan_version(min_version='2.9.0'):
            mappings = config.get('ckan.legacy_route_mappings', {})
            if isinstance(mappings, string_types):
                mappings = json.loads(mappings)

            mappings.update({
                'harvest_read': 'harvest.read',
                'harvest_edit': 'harvest.edit',
            })
            bp_routes = [
                "delete", "refresh", "admin", "about",
                "clear", "job_list", "job_show_last", "job_show",
                "job_abort", "object_show"
            ]
            mappings.update({
                'harvest_' + route: 'harvester.' + route
                for route in bp_routes
            })
            # https://github.com/ckan/ckan/pull/4521
            config['ckan.legacy_route_mappings'] = json.dumps(mappings)

    # IActions

    def get_actions(self):

        module_root = 'ckanext.harvest.logic.action'
        action_functions = _get_logic_functions(module_root)

        return action_functions

    # IAuthFunctions

    def get_auth_functions(self):

        module_root = 'ckanext.harvest.logic.auth'
        auth_functions = _get_logic_functions(module_root)

        return auth_functions

    # ITemplateHelpers

    def get_helpers(self):
        from ckanext.harvest import helpers as harvest_helpers
        return {
                'package_list_for_source': harvest_helpers.package_list_for_source,
                'package_count_for_source': harvest_helpers.package_count_for_source,
                'harvesters_info': harvest_helpers.harvesters_info,
                'harvester_types': harvest_helpers.harvester_types,
                'harvest_frequencies': harvest_helpers.harvest_frequencies,
                'link_for_harvest_object': harvest_helpers.link_for_harvest_object,
                'harvest_source_extra_fields': harvest_helpers.harvest_source_extra_fields,
                'bootstrap_version': harvest_helpers.bootstrap_version,
                'get_harvest_source': harvest_helpers.get_harvest_source,
                }

    def dataset_facets(self, facets_dict, package_type):

        if package_type != 'harvest':
            return facets_dict

        return OrderedDict([('frequency', 'Frequency'),
                            ('source_type', 'Type'),
                            ])

    def organization_facets(self, facets_dict, organization_type, package_type):

        if package_type != 'harvest':
            return facets_dict

        return OrderedDict([('frequency', 'Frequency'),
                            ('source_type', 'Type'),
                            ])


def _add_extra(data_dict, key, value):
    if 'extras' not in data_dict:
        data_dict['extras'] = []

    data_dict['extras'].append({
        'key': key, 'value': value, 'state': u'active'
    })


def _get_logic_functions(module_root, logic_functions={}):

    for module_name in ['get', 'create', 'update', 'patch', 'delete']:
        module_path = '%s.%s' % (module_root, module_name,)

        module = __import__(module_path)

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_') and (hasattr(value, '__call__')
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
            source.__setattr__(o, data_dict[o])

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
            source.__setattr__(f, data_dict[f])

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
        jobs = HarvestJob.filter(source=source, status=u'New')
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


def _configure_db_logger(config):
    # Log scope
    #
    # -1 - do not log to the database
    #  0 - log everything
    #  1 - model, logic.action, logic.validators, harvesters
    #  2 - model, logic.action, logic.validators
    #  3 - model, logic.action
    #  4 - logic.action
    #  5 - model
    #  6 - plugin
    #  7 - harvesters
    #
    scope = p.toolkit.asint(config.get('ckan.harvest.log_scope', -1))
    if scope == -1:
        return

    parent_logger = 'ckanext.harvest'
    children = ['plugin', 'model', 'logic.action.create', 'logic.action.delete',
                'logic.action.get',  'logic.action.patch', 'logic.action.update',
                'logic.validators', 'harvesters.base', 'harvesters.ckanharvester']

    children_ = {0: children, 1: children[1:], 2: children[1:-2],
                 3: children[1:-3], 4: children[2:-3], 5: children[1:2],
                 6: children[:1], 7: children[-2:]}

    # Get log level from config param - default: DEBUG
    from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
    level = config.get('ckan.harvest.log_level', 'debug').upper()
    if level == 'DEBUG':
        level = DEBUG
    elif level == 'INFO':
        level = INFO
    elif level == 'WARNING':
        level = WARNING
    elif level == 'ERROR':
        level = ERROR
    elif level == 'CRITICAL':
        level = CRITICAL
    else:
        level = DEBUG

    loggers = children_.get(scope)

    # Get root logger and set db handler
    logger = getLogger(parent_logger)
    if scope < 1:
        logger.addHandler(DBLogHandler(level=level))

    # Set db handler to all child loggers
    for _ in loggers:
        child_logger = logger.getChild(_)
        child_logger.addHandler(DBLogHandler(level=level))

# -*- coding: utf-8 -*-

import os
import json
from logging import getLogger

from collections import OrderedDict

from ckan import logic
from ckan import model
import ckan.plugins as p
from ckan.lib.plugins import DefaultDatasetForm

from ckan.lib.plugins import DefaultTranslation

import ckanext.harvest
from ckanext.harvest import cli, views
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvest.log import DBLogHandler

from ckanext.harvest.utils import (
    DATASET_TYPE_NAME
)

log = getLogger(__name__)
assert not log.disabled


class Harvest(p.SingletonPlugin, DefaultDatasetForm, DefaultTranslation):
    p.implements(p.IClick)
    p.implements(p.IBlueprint)
    p.implements(p.IConfigurable)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IDatasetForm)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IFacets, inherit=True)
    p.implements(p.ITranslation, inherit=True)

    startup = False

    # IClick

    def get_commands(self):
        return cli.get_commands()

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprints()

    # ITranslation
    def i18n_directory(self):
        u'''Change the directory of the .mo translation files'''
        return os.path.join(
            os.path.dirname(ckanext.harvest.__file__),
            'i18n'
        )

    # IPackageController

    # CKAN < 2.10 hooks
    def after_create(self, context, data_dict):
        return self.after_dataset_create(context, data_dict)

    def after_update(self, context, data_dict):
        return self.after_dataset_update(context, data_dict)

    def after_delete(self, context, data_dict):
        return self.after_dataset_delete(context, data_dict)

    def before_search(self, search_params):
        return self.before_dataset_search(search_params)

    def before_index(self, pkg_dict):
        return self.before_dataset_index(pkg_dict)

    def after_show(self, context, data_dict):
        return self.after_dataset_show(context, data_dict)

    # CKAN >= 2.10 hooks
    def after_dataset_create(self, context, data_dict):
        if (
            "type" in data_dict
            and data_dict["type"] == DATASET_TYPE_NAME
            and not self.startup
        ):
            # Create an actual HarvestSource object
            _create_harvest_source_object(context, data_dict)

    def after_dataset_update(self, context, data_dict):
        if "type" in data_dict and data_dict["type"] == DATASET_TYPE_NAME:
            # Edit the actual HarvestSource object
            _update_harvest_source_object(context, data_dict)

    def after_dataset_delete(self, context, data_dict):

        package_dict = p.toolkit.get_action("package_show")(
            context, {"id": data_dict["id"]}
        )

        if "type" in package_dict and package_dict["type"] == DATASET_TYPE_NAME:
            # Delete the actual HarvestSource object
            _delete_harvest_source_object(context, package_dict)

    def before_dataset_search(self, search_params):
        """Prevents the harvesters being shown in dataset search results."""

        fq = search_params.get("fq", "")
        if "dataset_type:harvest" not in fq:
            fq = "{0} -dataset_type:harvest".format(fq)
            search_params.update({"fq": fq})

        return search_params

    def _add_or_update_harvest_metadata(self, key, value, data_dict):
        """Adds extras fields or updates them if already exist."""
        if not data_dict.get("extras"):
            data_dict["extras"] = []

        for e in data_dict.get("extras"):
            if e.get("key") == key:
                e.update({"value": value})
                break
        else:
            data_dict["extras"].append({"key": key, "value": value})

    def before_dataset_index(self, pkg_dict):
        """Adds harvest metadata to the extra field of the dataset.

        This method will add or update harvest related metadata in `pkg_dict`,
        `data_dict` and `validated_data_dict` so it can be obtained when
        calling package_show API (that depends on Solr data). This metadata will
        be stored in the `extras` field of the dictionaries ONLY if it does not
        already exist in the root schema.

        Note: If another extension adds any harvest extra to the `package_show`
        schema then this method will not add them again in the `extras` field to avoid
        validation errors when updating a package.

        If the harvest extra has been added to the root schema, then we will not update
        them since it is responsibility of the package validators to do it.
        """
        # Fix to support Solr8
        if isinstance(pkg_dict.get('status'), dict):
            try:
                pkg_dict['status'] = json.dumps(pkg_dict['status'])
            except ValueError:
                pkg_dict.pop('status', None)

        harvest_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.package_id == pkg_dict["id"]) \
            .filter(
                HarvestObject.current == True # noqa
            ).order_by(HarvestObject.import_finished.desc()) \
            .first()

        if not harvest_object:
            return pkg_dict

        harvest_extras = [
            ("harvest_object_id", harvest_object.id),
            ("harvest_source_id", harvest_object.source.id),
            ("harvest_source_title", harvest_object.source.title),
        ]

        data_dict = json.loads(pkg_dict["data_dict"])
        for key, value in harvest_extras:
            if key in data_dict.keys():
                data_dict[key] = value
                continue
            self._add_or_update_harvest_metadata(key, value, data_dict)

        validated_data_dict = json.loads(pkg_dict["validated_data_dict"])
        for key, value in harvest_extras:
            if key in validated_data_dict.keys():
                validated_data_dict[key] = value
                continue
            self._add_or_update_harvest_metadata(key, value, validated_data_dict)

        # Add harvest extras to main indexed pkg_dict
        for key, value in harvest_extras:
            if key not in pkg_dict.keys():
                pkg_dict[key] = value

        pkg_dict["data_dict"] = json.dumps(data_dict)
        pkg_dict["validated_data_dict"] = json.dumps(validated_data_dict)

        return pkg_dict

    def after_dataset_show(self, context, data_dict):

        if "type" in data_dict and data_dict["type"] == DATASET_TYPE_NAME:
            # This is a harvest source dataset, add extra info from the
            # HarvestSource object
            source = HarvestSource.get(data_dict["id"])
            if not source:
                log.error(
                    "Harvest source not found for dataset {0}".format(data_dict["id"])
                )
                return data_dict

            st_action_name = "harvest_source_show_status"
            try:
                status_action = p.toolkit.get_action(st_action_name)
            except KeyError:
                logic.clear_actions_cache()
                status_action = p.toolkit.get_action(st_action_name)

            data_dict["status"] = status_action(context, {"id": source.id})

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
        from ckanext.harvest.logic.schema import harvest_source_create_package_schema, unicode_safe
        schema = harvest_source_create_package_schema()
        if self.startup:
            schema['id'] = [unicode_safe]
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

        # Configure database logger
        _configure_db_logger(config)

        self.startup = False

    def update_config(self, config):
        p.toolkit.add_template_directory(config, 'templates')
        p.toolkit.add_public_directory(config, 'public')
        p.toolkit.add_resource('assets', 'ckanext-harvest')
        p.toolkit.add_resource('public/ckanext/harvest/javascript', 'harvest-extra-field')

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

# -*- coding: utf-8 -*-

import six
import ckan.plugins as p

from ckan.logic.schema import default_extras_schema
from ckan.logic.validators import (package_id_exists,
                                   name_validator,
                                   owner_org_validator,
                                   package_name_validator,
                                   boolean_validator,
                                   )
from ckan.logic.converters import convert_to_extras, convert_from_extras

from ckan.lib.navl.validators import (ignore_missing,
                                      not_empty,
                                      ignore,
                                      if_empty_same_as,
                                      )

from ckanext.harvest.logic.validators import (harvest_source_url_validator,
                                              harvest_source_type_exists,
                                              harvest_source_config_validator,
                                              harvest_source_extra_validator,
                                              harvest_source_frequency_exists,
                                              dataset_type_exists,
                                              harvest_source_convert_from_config,
                                              harvest_source_id_exists,
                                              harvest_job_exists,
                                              harvest_object_extras_validator,
                                              )


def harvest_source_schema():

    schema = {
        'id': [ignore_missing, six.text_type, package_id_exists],
        'type': [dataset_type_exists, six.text_type],
        'url': [not_empty, six.text_type, harvest_source_url_validator],
        'name': [not_empty, six.text_type, name_validator, package_name_validator],
        'source_type': [not_empty, six.text_type, harvest_source_type_exists, convert_to_extras],
        'title': [if_empty_same_as("name"), six.text_type],
        'notes': [ignore_missing, six.text_type],
        'owner_org': [owner_org_validator, six.text_type],
        'private': [ignore_missing, boolean_validator],
        'organization': [ignore_missing],
        'frequency': [ignore_missing, six.text_type, harvest_source_frequency_exists, convert_to_extras],
        'state': [ignore_missing],
        'config': [ignore_missing, harvest_source_config_validator, convert_to_extras],
        'extras': default_extras_schema(),
    }

    extras_schema = default_extras_schema()
    extras_schema['__extras'] = [ignore]

    schema['extras'] = extras_schema

    if p.toolkit.check_ckan_version('2.2'):
        from ckan.logic.validators import datasets_with_no_organization_cannot_be_private
        schema['private'].append(datasets_with_no_organization_cannot_be_private)

    return schema


def harvest_source_create_package_schema():

    schema = harvest_source_schema()
    schema['__extras'] = [harvest_source_extra_validator]
    schema['save'] = [ignore]
    schema.pop("id")

    return schema


def harvest_source_update_package_schema():

    schema = harvest_source_create_package_schema()
    schema['owner_org'] = [ignore_missing, owner_org_validator, six.text_type]

    return schema


def harvest_source_show_package_schema():

    schema = harvest_source_schema()
    schema.update({
        'source_type': [convert_from_extras, ignore_missing],
        'frequency': [convert_from_extras, ignore_missing],
        'config': [convert_from_extras, harvest_source_convert_from_config, ignore_missing],
        'metadata_created': [],
        'metadata_modified': [],
        'owner_org': [],
        'creator_user_id': [],
        'organization': [],
        'notes': [],
        'revision_id': [ignore_missing],
        'revision_timestamp': [ignore_missing],
        'tracking_summary': [ignore_missing],
    })

    schema['__extras'] = [ignore]

    return schema


def harvest_object_create_schema():
    schema = {
        'guid': [ignore_missing, six.text_type],
        'content': [ignore_missing, six.text_type],
        'state': [ignore_missing, six.text_type],
        'job_id': [harvest_job_exists],
        'source_id': [ignore_missing, harvest_source_id_exists],
        'package_id': [ignore_missing, package_id_exists],
        'extras': [ignore_missing, harvest_object_extras_validator],
    }
    return schema

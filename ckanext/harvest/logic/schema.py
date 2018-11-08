import ckan.plugins as p

from ckan.logic.schema import default_extras_schema
from ckan.logic.validators import (package_id_exists,
                                   name_validator,
                                   owner_org_validator,
                                   package_name_validator,
                                   ignore_not_package_admin,
                                   boolean_validator,
                                   )
from ckan.logic.converters import convert_to_extras, convert_from_extras

from ckan.lib.navl.validators import (ignore_missing,
                                      not_empty,
                                      ignore,
                                      if_empty_same_as,
                                      )

from ckan.logic.schema import default_tags_schema

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
import ckan.plugins.toolkit as toolkit
from ckanext.dgua.validators import dgua_tag_string_convert, dgua_tag_name_validator, dgua_tags_to_string_convert

def harvest_source_schema():

    schema = {
        'id': [ignore_missing, unicode, package_id_exists],
        'type': [dataset_type_exists, unicode],
        'url': [not_empty, unicode, harvest_source_url_validator],
        'name': [not_empty, unicode, name_validator, package_name_validator],
        'source_type': [not_empty, unicode, harvest_source_type_exists, convert_to_extras],
        'title': [if_empty_same_as("name"), unicode],
        'notes': [ignore_missing, unicode],
        'owner_org': [owner_org_validator, unicode],
        'private': [ignore_missing, boolean_validator],
        'organization': [ignore_missing],
        'frequency': [ignore_missing, unicode, harvest_source_frequency_exists, convert_to_extras],
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

def dgua_tags_schema():
    schema = default_tags_schema()
    schema.update({
        'name': [toolkit.get_validator('not_missing'),
                 toolkit.get_validator('not_empty'),
                 unicode,
                 toolkit.get_validator('tag_length_validator'),
                 dgua_tag_name_validator]
    })
    return schema

def harvest_source_create_package_schema():

    schema = harvest_source_schema()
    schema['__extras'] = [harvest_source_extra_validator]
    schema['save'] = [ignore]
    schema.pop("id")
    schema.update({
        'title': [not_empty],
        'notes': [not_empty],
        'update_frequency': [not_empty, convert_to_extras],
        'purpose_of_collecting_information': [not_empty, toolkit.get_validator('max_length')(512), convert_to_extras],
        'tag_string': [not_empty, convert_to_extras],
        'tags': dgua_tags_schema(),
        'language': [not_empty, convert_to_extras],
        'is_datapackage': [ignore_missing, convert_to_extras]
    })

    return schema

def harvest_source_update_package_schema():

    schema = harvest_source_create_package_schema()
    schema['owner_org'] = [ignore_missing, owner_org_validator, unicode]

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
        'revision_id': [],
        'revision_timestamp': [ignore_missing],
        'tracking_summary': [ignore_missing],
        'update_frequency': [
            toolkit.get_converter('convert_from_extras'),
            toolkit.get_validator('ignore_missing')],
        'purpose_of_collecting_information': [
            toolkit.get_converter('convert_from_extras'),
            toolkit.get_validator('ignore_missing')],
        'language': [
            toolkit.get_converter('convert_from_extras'),
            toolkit.get_validator('not_empty')],
        'is_datapackage': [
            toolkit.get_converter('convert_from_extras'),
            toolkit.get_validator('ignore_missing')],
        'tag_string': [convert_from_extras,
            toolkit.get_validator('not_empty')
        ],
    })

    schema['__extras'] = [ignore]

    return schema

def harvest_object_create_schema():
    schema = {
        'guid': [ignore_missing, unicode],
        'content': [ignore_missing, unicode],
        'state': [ignore_missing, unicode],
        'job_id': [harvest_job_exists],
        'source_id': [ignore_missing, harvest_source_id_exists],
        'package_id': [ignore_missing, package_id_exists],
        'extras': [ignore_missing, harvest_object_extras_validator],
    }
    return schema


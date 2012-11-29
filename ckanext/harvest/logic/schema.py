from ckan.lib.base import config
from ckan.logic.validators import (package_id_exists,
                                   name_validator,
                                   package_name_validator,
                                   )
from ckan.logic.converters import convert_to_extras

from ckan.lib.navl.validators import (ignore_missing,
                                      not_empty,
                                      ignore,
                                      if_empty_same_as,
                                     )

from ckanext.harvest.logic.validators import (harvest_source_id_exists,
                                            harvest_source_url_validator,
                                            harvest_source_type_exists,
                                            harvest_source_config_validator,
                                            harvest_source_active_validator,
                                            harvest_source_frequency_exists,
                                            dataset_type_exists,)
#TODO: remove
def old_default_harvest_source_schema():

    schema = {
        'id': [ignore_missing, unicode, harvest_source_id_exists],
        'url': [not_empty, unicode, harvest_source_url_validator],
        'type': [not_empty, unicode, harvest_source_type_exists],
        'title': [ignore_missing,unicode],
        'description': [ignore_missing,unicode],
        'frequency': [ignore_missing,unicode, harvest_source_frequency_exists],
        'active': [ignore_missing,harvest_source_active_validator],
        'user_id': [ignore_missing,unicode],
        'config': [ignore_missing,harvest_source_config_validator]
    }

    if config.get('ckan.harvest.auth.profile',None) == 'publisher':
        schema['publisher_id'] = [not_empty,unicode]
    else:
        schema['publisher_id'] = [ignore_missing,unicode]

    return schema

#TODO: remove
def old_harvest_source_form_schema():

    schema = old_default_harvest_source_schema()
    schema['save'] = [ignore]

    return schema

def harvest_source_schema():

    schema = {
        'id': [ignore_missing, unicode, package_id_exists],
        'type': [dataset_type_exists, unicode],
        'url': [not_empty, unicode, harvest_source_url_validator],
        'name': [not_empty, unicode, name_validator, package_name_validator],
        'source_type': [not_empty, unicode, harvest_source_type_exists, convert_to_extras],
        'title': [if_empty_same_as("name"), unicode],
        'notes': [ignore_missing, unicode],
        'frequency': [ignore_missing, unicode, harvest_source_frequency_exists, convert_to_extras],
        'state': [ignore_missing, harvest_source_active_validator],
        'config': [ignore_missing, harvest_source_config_validator, convert_to_extras]
    }

    return schema

def harvest_source_form_schema():

    schema = harvest_source_schema()

    schema['save'] = [ignore]
    schema.pop("id")

    return schema


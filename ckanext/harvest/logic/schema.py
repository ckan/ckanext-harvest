from ckan.logic.schema import default_extras_schema
from ckan.logic.validators import (package_id_exists,
                                   name_validator,
                                   owner_org_validator,
                                   package_name_validator,
                                   ignore_not_package_admin,
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
                                              harvest_source_frequency_exists,
                                              dataset_type_exists,
                                              )

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
        'frequency': [ignore_missing, unicode, harvest_source_frequency_exists, convert_to_extras],
        'state': [ignore_missing],
        'config': [ignore_missing, harvest_source_config_validator, convert_to_extras],
        'extras': default_extras_schema(),
        '__extras': [ignore],
    }

    extras_schema = default_extras_schema()
    extras_schema['__extras'] = [ignore]

    schema['extras'] = extras_schema

    return schema

def harvest_source_form_to_db_schema():

    schema = harvest_source_schema()

    schema['save'] = [ignore]
    schema.pop("id")

    return schema

def harvest_source_db_to_form_schema():

    schema = harvest_source_schema()
    schema.update({
        'source_type': [convert_from_extras, ignore_missing],
        'frequency': [convert_from_extras, ignore_missing],
        'config': [convert_from_extras, ignore_missing],
        'owner_org': [ignore_missing]
    })

    return schema

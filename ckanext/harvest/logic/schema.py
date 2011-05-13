from ckan.lib.navl.validators import (ignore_missing,
                                      not_empty,
                                      empty,
                                      ignore,
                                      not_missing
                                     )

from ckanext.harvest.logic.validators import harvest_source_id_exists, \
                                            harvest_source_url_validator, \
                                            harvest_source_type_exists

def default_harvest_source_schema():

    schema = {
        'id': [ignore_missing, unicode, harvest_source_id_exists],
        'url': [not_empty, unicode, harvest_source_url_validator],
        'type': [not_empty, unicode, harvest_source_type_exists],
        'description': [ignore_missing],
        'active': [ignore_missing],
        'user_id': [ignore_missing],
        'publisher_id': [ignore_missing],
        #'config'
    }

    return schema


def harvest_source_form_schema():

    schema = default_harvest_source_schema()
    schema['save'] = [ignore]

    return schema

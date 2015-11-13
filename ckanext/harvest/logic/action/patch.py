'''API functions for partial updates of existing data in CKAN'''

import logging
from ckan.logic import get_action
from ckanext.harvest.plugin import DATASET_TYPE_NAME

log = logging.getLogger(__name__)


def harvest_source_patch(context, data_dict):
    '''
    Patch an existing harvest source

    This method just proxies the request to package_patch, which will update a
    harvest_source dataset type and the HarvestSource object. All auth checks
    and validation will be done there. We only make sure to set the dataset
    type.

    Note that the harvest source type (ckan, waf, csw, etc) is now set via the
    source_type field.

    All fields that are not provided, will be stay as they were before.

    :param id: the name or id of the harvest source to update
    :type id: string
    :param url: the URL for the harvest source
    :type url: string
    :param name: the name of the new harvest source, must be between 2 and 100
        characters long and contain only lowercase alphanumeric characters
    :type name: string
    :param title: the title of the dataset (optional, default: same as
        ``name``)
    :type title: string
    :param notes: a description of the harvest source (optional)
    :type notes: string
    :param source_type: the harvester type for this source. This must be one
        of the registerd harvesters, eg 'ckan', 'csw', etc.
    :type source_type: string
    :param frequency: the frequency in wich this harvester should run. See
        ``ckanext.harvest.model`` source for possible values. Default is
        'MANUAL'
    :type frequency: string
    :param config: extra configuration options for the particular harvester
        type. Should be a serialized as JSON. (optional)
    :type config: string

    :returns: the updated harvest source
    :rtype: dictionary
    '''
    log.info('Patch harvest source: %r', data_dict)

    data_dict['type'] = DATASET_TYPE_NAME

    context['extras_as_string'] = True
    try:
        source = get_action('package_patch')(context, data_dict)
    except KeyError:
        raise Exception('The harvest_source_patch action is not available on '
                        'this version of CKAN')

    return source

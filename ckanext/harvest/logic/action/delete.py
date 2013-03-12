import logging

from ckan import plugins as p


log = logging.getLogger(__name__)

def harvest_source_delete(context, data_dict):
    '''
    Deletes an existing harvest source

    This method just proxies the request to package_delete,
    which will delete the actual harvest type dataset and the
    HarvestSource object (via the after_delete extension point).

    :param id: the name or id of the harvest source to delete
    :type id: string

    :returns: the newly created harvest source
    :rtype: dictionary

    '''
    log.info('Deleting harvest source: %r', data_dict)

    p.toolkit.check_access('harvest_source_delete', context, data_dict)

    p.toolkit.get_action('package_delete')(context, data_dict)

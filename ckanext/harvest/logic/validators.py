import logging
import urlparse
import json

from ckan.lib.navl.dictization_functions import Invalid, validate
from ckan import model
from ckan.plugins import PluginImplementations

from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.model import HarvestSource, UPDATE_FREQUENCIES, HarvestJob
from ckanext.harvest.interfaces import IHarvester

from ckan.lib.navl.validators import keep_extras

log = logging.getLogger(__name__)


def harvest_source_id_exists(value, context):

    result = HarvestSource.get(value)

    if not result:
        raise Invalid('Harvest Source with id %r does not exist.' % str(value))
    return value


def harvest_job_exists(value, context):
    '''Check if a harvest job exists and returns the model if it does'''
    result = HarvestJob.get(value)

    if not result:
        raise Invalid('Harvest Job with id %r does not exist.' % str(value))
    return result


def _normalize_url(url):
    o = urlparse.urlparse(url)

    # Normalize port
    if ':' in o.netloc:
        parts = o.netloc.split(':')
        if (o.scheme == 'http' and parts[1] == '80') or \
           (o.scheme == 'https' and parts[1] == '443'):
            netloc = parts[0]
        else:
            netloc = ':'.join(parts)
    else:
        netloc = o.netloc

    # Remove trailing slash
    path = o.path.rstrip('/')

    check_url = urlparse.urlunparse((
        o.scheme,
        netloc,
        path,
        None, None, None))

    return check_url


def harvest_source_url_validator(key, data, errors, context):
    '''Validate the provided harvest source URL

    Checks that the URL & config combination are unique to this HarvestSource.
    '''

    package = context.get('package')

    if package:
        package_id = package.id
    else:
        package_id = data.get(key[:-1] + ('id',))

    try:
        new_config = data.get(key[:-1] + ('config',))
    except:
        new_config = None

    new_url = _normalize_url(data[key])

    q = model.Session.query(model.Package.id, model.Package.url) \
             .filter(model.Package.type == DATASET_TYPE_NAME)

    if package_id:
        # When editing a source we need to avoid its own URL
        q = q.filter(model.Package.id != package_id)

    existing_sources = q.all()

    for id_, url in existing_sources:
        url = _normalize_url(url)
        conf = model.Session.query(HarvestSource.config).filter(
            HarvestSource.id == id_).first()
        if conf:
            conf = conf[0]
        else:
            conf = None

        if url == new_url and conf == new_config:
            raise Invalid('There already is a Harvest Source for this URL (& '
                          'config): url=%s config=%s' % (new_url, new_config))

    return data[key]


def harvest_source_type_exists(value, context):
    # TODO: use new description interface

    # Get all the registered harvester types
    available_types = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %s does not provide the harvester name in '
                      'the info response' % harvester)
            continue
        available_types.append(info['name'])

    if not value in available_types:
        raise Invalid('Unknown harvester type: %s. Have you registered a '
                      'harvester for this type?' % value)

    return value


def harvest_source_config_validator(key, data, errors, context):
    harvester_type = data.get(('source_type',), '')
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if info['name'] == harvester_type:
            if hasattr(harvester, 'validate_config'):
                try:
                    return harvester.validate_config(data[key])
                except Exception, e:
                    raise Invalid('Error parsing the configuration options: %s'
                                  % e)
            else:
                return data[key]


def keep_not_empty_extras(key, data, errors, context):
    extras = data.pop(key, {})
    for extras_key, value in extras.iteritems():
        if value:
            data[key[:-1] + (extras_key,)] = value


def harvest_source_extra_validator(key, data, errors, context):
    harvester_type = data.get(('source_type',), '')

    # gather all extra fields to use as whitelist of what
    # can be added to top level data_dict
    all_extra_fields = set()
    for harvester in PluginImplementations(IHarvester):
        if not hasattr(harvester, 'extra_schema'):
            continue
        all_extra_fields.update(harvester.extra_schema().keys())

    extra_schema = {'__extras': [keep_not_empty_extras]}
    for harvester in PluginImplementations(IHarvester):
        if not hasattr(harvester, 'extra_schema'):
            continue
        info = harvester.info()
        if not info['name'] == harvester_type:
            continue
        extra_schema.update(harvester.extra_schema())
        break

    extra_data, extra_errors = validate(data.get(key, {}), extra_schema)
    for key in extra_data.keys():
        # only allow keys that appear in at least one harvester
        if key not in all_extra_fields:
            extra_data.pop(key)

    for key, value in extra_data.iteritems():
        data[(key,)] = value

    for key, value in extra_errors.iteritems():
        errors[(key,)] = value

    # need to get config out of extras as __extra runs
    # after rest of validation
    package_extras = data.get(('extras',), [])

    for num, extra in enumerate(list(package_extras)):
        if extra['key'] == 'config':
            # remove config extra so we can add back cleanly later
            package_extras.pop(num)
            try:
                config_dict = json.loads(extra.get('value') or '{}')
            except ValueError:
                log.error('Wrong JSON provided in config, skipping')
                config_dict = {}
            break
    else:
        config_dict = {}
    config_dict.update(extra_data)
    if config_dict and not extra_errors:
        config = json.dumps(config_dict)
        package_extras.append(dict(key='config',
                                   value=config))
        data[('config',)] = config
    if package_extras:
        data[('extras',)] = package_extras


def harvest_source_convert_from_config(key, data, errors, context):
    config = data[key]
    if config:
        config_dict = json.loads(config)
        for key, value in config_dict.iteritems():
            data[(key,)] = value


def harvest_source_active_validator(value, context):
    if isinstance(value, basestring):
        if value.lower() == 'true':
            return True
        else:
            return False
    return bool(value)


def harvest_source_frequency_exists(value):
    if value == '':
        value = 'MANUAL'
    if value.upper() not in UPDATE_FREQUENCIES:
        raise Invalid('Frequency %s not recognised' % value)
    return value.upper()


def dataset_type_exists(value):
    if value != DATASET_TYPE_NAME:
        value = DATASET_TYPE_NAME
    return value


def harvest_object_extras_validator(value, context):
    if not isinstance(value, dict):
        raise Invalid('extras must be a dict')
    for v in value.values():
        if not isinstance(v, basestring):
            raise Invalid('extras must be a dict of strings')
    return value

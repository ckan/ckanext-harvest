import urlparse

from ckan.lib.navl.dictization_functions import Invalid, missing
from ckan.model import Session
from ckan.plugins import PluginImplementations

from ckanext.harvest.model import HarvestSource
from ckanext.harvest.interfaces import IHarvester
 

#TODO: use context?

def harvest_source_id_exists(value, context):
    
    result = HarvestSource.get(value,None)

    if not result:
        raise Invalid('Harvest Source with id %r does not exist.' % str(value))
    return value

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
            None,None,None))

    return check_url

def harvest_source_url_validator(key,data,errors,context):
    new_url = _normalize_url(data[key])
    source_id = data.get(('id',),'')
    if source_id:
        # When editing a source we need to avoid its own URL
        existing_sources = Session.query(HarvestSource.url,HarvestSource.active) \
                       .filter(HarvestSource.id!=source_id).all()
    else:
        existing_sources = Session.query(HarvestSource.url,HarvestSource.active).all()

    for url,active in existing_sources:
        url = _normalize_url(url)
        if url == new_url:
            raise Invalid('There already is a Harvest Source for this URL: %s' % data[key])

    return data[key] 

def harvest_source_type_exists(value,context):
    #TODO: use new description interface

    # Get all the registered harvester types
    available_types = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %r does not provide the harvester name in the info response' % str(harvester))
            continue
        available_types.append(info['name'])


    if not value in available_types:
        raise Invalid('Unknown harvester type: %s. Have you registered a harvester for this type?' % value)
    
    return value

def harvest_source_config_validator(key,data,errors,context):
    harvester_type = data.get(('type',),'')
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if info['name'] == harvester_type:
            if hasattr(harvester, 'validate_config'):
                try:
                    return harvester.validate_config(data[key])
                except Exception, e:
                    raise Invalid('Error parsing the configuration options: %s' % str(e))
            else:
                return data[key]

def harvest_source_active_validator(value,context):
    if isinstance(value,basestring):
        if value.lower() == 'true':
            return True
        else:
            return False
    return bool(value)


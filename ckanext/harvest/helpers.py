
from pylons import request
from ckan import logic
from ckan import model
import ckan.lib.helpers as h
import ckan.plugins as p

from ckanext.harvest.model import UPDATE_FREQUENCIES
from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.interfaces import IHarvester

def package_list_for_source(source_id):
    '''
    Creates a dataset list with the ones belonging to a particular harvest
    source.

    It calls the package_list snippet and the pager.
    '''
    limit = 20
    page = int(request.params.get('page', 1))
    fq = 'harvest_source_id:"{0}"'.format(source_id)
    search_dict = {
        'fq' : fq,
        'rows': limit,
        'sort': 'metadata_modified desc',
        'start': (page - 1) * limit,
    }

    context = {'model': model, 'session': model.Session}

    owner_org =  p.toolkit.c.harvest_source.get('owner_org', '')
    if owner_org:
        user_member_of_orgs = [org['id'] for org
                   in h.organizations_available('read')]
        if (p.toolkit.c.harvest_source and owner_org in user_member_of_orgs):
            context['ignore_capacity_check'] = True

    query = logic.get_action('package_search')(context, search_dict)

    base_url = h.url_for('{0}_read'.format(DATASET_TYPE_NAME), id=source_id)
    def pager_url(q=None, page=None):
        url = base_url
        if page:
            url += '?page={0}'.format(page)
        return url

    pager = h.Page(
        collection=query['results'],
        page=page,
        url=pager_url,
        item_count=query['count'],
        items_per_page=limit
    )
    pager.items = query['results']

    if query['results']:
        out = h.snippet('snippets/package_list.html', packages=query['results'])
        out += pager.pager()
    else:
        out = h.snippet('snippets/package_list_empty.html')

    return out

def harvesters_info():
    context = {'model': model, 'user': p.toolkit.c.user or p.toolkit.c.author}
    return logic.get_action('harvesters_info_show')(context,{})

def harvester_types():
    harvesters = harvesters_info()
    return [{'text': p.toolkit._(h['title']), 'value': h['name']}
            for h in harvesters]

def harvest_frequencies():

    return [{'text': p.toolkit._(f.title()), 'value': f}
            for f in UPDATE_FREQUENCIES]

def link_for_harvest_object(id=None, guid=None, text=None):

    if not id and not guid:
        return None

    if guid:
        context = {'model': model, 'user': p.toolkit.c.user or p.toolkit.c.author}
        obj =logic.get_action('harvest_object_show')(context, {'id': guid, 'attr': 'guid'})
        id = obj.id

    url = h.url_for('harvest_object_show', id=id)
    text = text or guid or id
    link = '<a href="{url}">{text}</a>'.format(url=url, text=text)

    return p.toolkit.literal(link)

def harvest_source_extra_fields():
    fields = {}
    for harvester in p.PluginImplementations(IHarvester):
        if not hasattr(harvester, 'extra_schema'):
            continue
        fields[harvester.info()['name']] = harvester.extra_schema().keys()
    return fields



from pylons import request
from ckan import logic
from ckan import model
import ckan.lib.helpers as h

def package_list_for_source(source_id):
    '''
    Creates a dataset list with the ones belonging to a particular harvest
    source.

    It calls the package_list snippet and the pager.
    '''
    limit = 20
    page = int(request.params.get('page', 1))
    fq = 'harvest_source_id:{0}'.format(source_id)
    search_dict = {
        'fq' : fq,
        'rows': 10,
        'sort': 'metadata_modified desc',
        'start': (page - 1) * limit,
    }

    context = {'model': model, 'session': model.Session}
    query = logic.get_action('package_search')(context, search_dict)

    pager = h.Page(
        collection=query['results'],
        page=page,
        #        url=pager_url,
        item_count=query['count'],
        items_per_page=limit
    )
    pager.items = query['results']

    out = h.snippet('snippets/package_list.html', packages=query['results'])
    out += pager.pager()

    return out

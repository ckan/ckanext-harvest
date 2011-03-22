# this is a namespace package
try:
    import pkg_resources
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__, __name__)

import os
from logging import getLogger

from genshi.input import HTML
from genshi.filters import Transformer

import ckan.lib.helpers as h

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes, IConfigurer
from ckan.plugins import IConfigurable, IGenshiStreamFilter

import html

log = getLogger(__name__)

class Harvest(SingletonPlugin):
    
    implements(IConfigurable)
    implements(IGenshiStreamFilter)
    implements(IRoutes, inherit=True)
    implements(IConfigurer, inherit=True)
    
    def configure(self, config):
        pass
        #self.enable_organizations = config.get('qa.organizations', False)
    
    def filter(self, stream):
        from pylons import request, tmpl_context as c
        routes = request.environ.get('pylons.routes_dict')

        if routes.get('controller') == 'package' and \
            routes.get('action') == 'read' and c.pkg.id:

            is_inspire = (c.pkg.extras.get('INSPIRE') == 'True')
            # TODO: What about WFS, WCS...
            is_wms = (c.pkg.extras.get('resource-type') == 'service')
            if is_inspire and is_wms:
                data = {'name': c.pkg.name}
                stream = stream | Transformer('body//div[@class="resources subsection"]')\
                    .append(HTML(html.MAP_VIEW % data))


        return stream

    def before_map(self, map):
        map.connect('harvest', '/harvest',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='index')
            
        map.connect('harvest_create_form', '/harvest/create',
            controller='ckanext.harvest.controllers.view:ViewController',
            conditions=dict(method=['GET']),
            action='create')

        map.connect('harvest_create', '/harvest/create',
            controller='ckanext.harvest.controllers.view:ViewController',
            conditions=dict(method=['POST']),
            action='create')

        map.connect('harvest_show', '/harvest/:id',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='show')

        map.connect('harvest_edit', '/harvest/:id/edit',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='edit')

        map.connect('harvest_delete', '/harvest/:id/delete',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='delete')

        map.connect('harvesting_job_create', '/harvest/:id/refresh',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='create_harvesting_job')

        map.connect('map_view', '/package/:id/map',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='map_view')

        map.connect('proxy', '/proxy',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='proxy')

        map.connect('api_spatial_query', '/api/2/search/package/geo',
            controller='ckanext.harvest.controllers.api:ApiController',
            action='spatial_query')
      
        return map

    def update_config(self, config):
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))

        template_dir = os.path.join(rootdir, 'templates')
        public_dir = os.path.join(rootdir, 'public')
        
        if config.get('extra_template_paths'):
            config['extra_template_paths'] += ','+template_dir
        else:
            config['extra_template_paths'] = template_dir
        if config.get('extra_public_paths'):
            config['extra_public_paths'] += ','+public_dir
        else:
            config['extra_public_paths'] = public_dir


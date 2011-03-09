import simplejson as json
import urllib
import ckan.lib.helpers as h
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort
#from ..dictization import *

class ViewController(BaseController):

    api_url = config.get('ckan.api_url', '/').rstrip('/')+'/api/2/rest'  
    
    def index(self):
        """
            TODO: error handling
        """
        # Request all harvesting sources
        sources_url = self.api_url + '/harvestsource'
        r = urllib.urlopen(sources_url).read()
        sources_ids = json.loads(r)
        
        source_url = sources_url + '/%s'
        sources = []
        for source_id in sources_ids:
            r = urllib.urlopen(source_url % source_id).read()
            sources.append(json.loads(r))
        
        c.sources = sources
        return render('ckanext/harvest/index.html')
        
    def create(self):
        # This is the DGU form API, so we don't use self.api_url
        form_url = config.get('ckan.api_url', '/').rstrip('/') + \
                   '/api/2/form/harvestsource/create'
        if request.method == 'GET':
            c.form = urllib.urlopen(form_url).read()
            return render('ckanext/harvest/create.html')
        if request.method == 'POST':
            #raw_post_data = request.environ['wsgi.input'].read(int(request.environ['CONTENT_LENGTH']))
            raw_post_data = request.environ['wsgi.input'].read()
            r = urllib.urlopen(form_url,raw_post_data)

            return str(r.getcode())

    def show(self,id):
        sources_url = self.api_url + '/harvestsource/%s' % id 
        r = urllib.urlopen(sources_url).read()
        c.source = json.loads(r)

        return render('ckanext/harvest/show.html')

import simplejson as json
import urllib2
import ckan.lib.helpers as h
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort
#from ..dictization import *

class ViewController(BaseController):

    api_url = config.get('ckan.api_url', '/').rstrip('/')+'/api/2/rest'
    
    def _request_document(self,url,data = False):
        try:
            doc = urllib2.urlopen(url).read()
            return doc
        except urllib2.URLError:
            #TODO: log?
            abort(500)

    def index(self):
        """
            TODO: error handling
        """
        # Request all harvesting sources
        sources_url = self.api_url + '/harvestsource'
        
        doc = self._request_document(sources_url)
        sources_ids = json.loads(doc)

        source_url = sources_url + '/%s'
        sources = []
        for source_id in sources_ids:
            doc = self._request_document(source_url % source_id)
            sources.append(json.loads(doc))

        c.sources = sources
        return render('ckanext/harvest/index.html')

    def create(self):
        # This is the DGU form API, so we don't use self.api_url
        form_url = config.get('ckan.api_url', '/').rstrip('/') + \
                   '/api/2/form/harvestsource/create'

        if request.method == 'GET':
            c.form = self._request_document(form_url)
            return render('ckanext/harvest/create.html')
        if request.method == 'POST':
            """
            TODO: Authz
            """

            #raw_post_data = request.environ['wsgi.input'].read(int(request.environ['CONTENT_LENGTH']))
            raw_post_data = request.environ['wsgi.input'].read()
            r = urllib2.urlopen(form_url,raw_post_data)

            return str(r.getcode())

    def show(self,id):
        sources_url = self.api_url + '/harvestsource/%s' % id
        doc = self._request_document(sources_url)
        c.source = json.loads(doc)

        return render('ckanext/harvest/show.html')

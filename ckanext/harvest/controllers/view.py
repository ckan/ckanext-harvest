from ckan.lib.helpers import json
import urllib2
import ckan.lib.helpers as h
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort, redirect
#from ..dictization import *

class ViewController(BaseController):

    api_url = config.get('ckan.api_url', 'http://localhost:5000').rstrip('/')+'/api/2/rest'
    
    def index(self):
        try:
            # Request all harvesting sources
            sources_url = self.api_url + '/harvestsource'
            
            doc = urllib2.urlopen(sources_url).read()
            sources_ids = json.loads(doc)

            source_url = sources_url + '/%s'
            sources = []
            
            # For each source, request its details
            for source_id in sources_ids:
                doc = urllib2.urlopen(source_url % source_id).read()
                sources.append(json.loads(doc))

            c.sources = sources
            return render('ckanext/harvest/index.html')
        except urllib2.HTTPError as e:
            raise Exception('The forms API returned an error:' + str(e.getcode()) + ' ' + e.msg )
    
    def create(self):

        # This is the DGU form API, so we don't use self.api_url
        form_url = config.get('ckan.api_url', '/').rstrip('/') + \
                   '/api/2/form/harvestsource/create'

        # Create a Request object to define the Authz header
        http_request = urllib2.Request(
            url = form_url,
            headers = {'Authorization' : config.get('ckan.harvesting.api_key')}
        )

        if request.method == 'GET':
            # Request the fields
            c.form = urllib2.urlopen(http_request).read()

            return render('ckanext/harvest/create.html')
        if request.method == 'POST':
            # Build an object like the one expected by the DGU form API
            data = {
                'form_data':
                    {'HarvestSource--url': request.POST['HarvestSource--url'],
                     'HarvestSource--description': request.POST['HarvestSource--description']},
                'user_ref':'',
                'publisher_ref':''
            }
            data = json.dumps(data)
            http_request.add_data(data)

            try:
                r = urllib2.urlopen(http_request)
    
                h.flash_success('Harvesting source added successfully')
                redirect(h.url_for(controller='harvest', action='index'))
            except urllib2.HTTPError as e:
                h.flash_error('An error occurred: ' + str(e.getcode()) + ' ' + e.msg)
                redirect(h.url_for(controller='harvest', action='create'))
                """
                if e.getcode() == 403:
                    abort(403)
                else:
                    raise Exception('The forms API returned an error:' + str(e.getcode()) + ' ' + e.msg )
                """
        

    def show(self,id):
        sources_url = self.api_url + '/harvestsource/%s' % id
        try:
            doc = urllib2.urlopen(sources_url).read()
        except urllib2.HTTPError as e:
            raise Exception('The forms API returned an error:' + str(e.getcode()) + ' ' + e.msg )       
        c.source = json.loads(doc)

        return render('ckanext/harvest/show.html')

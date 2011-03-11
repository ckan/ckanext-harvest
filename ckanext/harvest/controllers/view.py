from ckan.lib.helpers import json
import urllib2
import ckan.lib.helpers as h
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort, redirect
#from ..dictization import *

class ViewController(BaseController):

    api_url = config.get('ckan.api_url', 'http://localhost:5000').rstrip('/')+'/api/2/rest'
    form_api_url = config.get('ckan.api_url', 'http://localhost:5000').rstrip('/')+'/api/2/form'
    api_key = config.get('ckan.harvesting.api_key')

    def _do_request(self,url,data = None):

        http_request = urllib2.Request(
            url = url,
            headers = {'Authorization' : self.api_key}
        )

        if data:
            http_request.add_data(data)
        
        try:
            return urllib2.urlopen(http_request)
        except urllib2.HTTPError as e:
            raise


    def index(self):
        # Request all harvesting sources
        sources_url = self.api_url + '/harvestsource'
        
        doc = self._do_request(sources_url).read()
        sources_ids = json.loads(doc)

        source_url = sources_url + '/%s'
        sources = []
        
        # For each source, request its details
        for source_id in sources_ids:
            doc = self._do_request(source_url % source_id).read()
            sources.append(json.loads(doc))

        c.sources = sources
        return render('ckanext/harvest/index.html')
    
    def create(self):

        # This is the DGU form API, so we don't use self.api_url
        form_url = self.form_api_url + '/harvestsource/create'
        if request.method == 'GET':
            # Request the fields
            c.form = self._do_request(form_url).read()

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
            try:
                r = self._do_request(form_url,data)
  
                h.flash_success('Harvesting source added successfully')
            except urllib2.HTTPError as e:
                msg = 'An error occurred: [%s %s]' % (str(e.getcode()),e.msg)
                # The form API returns just a 500, so we are not exactly sure of what 
                # happened, but most probably it was a duplicate entry
                msg = msg + ' Does the source already exist?'
                h.flash_error(msg)
            finally:
                redirect(h.url_for(controller='harvest', action='index'))
 
    def show(self,id):
        sources_url = self.api_url + '/harvestsource/%s' % id
        doc = self._do_request(sources_url).read()
        c.source = json.loads(doc)
        
        return render('ckanext/harvest/show.html')

    def delete(self,id):
        form_url = self.form_api_url + '/harvestsource/delete/%s' % id
        r = self._do_request(form_url)
    
        h.flash_success('Harvesting source deleted successfully')
        redirect(h.url_for(controller='harvest', action='index', id=None))

    def create_harvesting_job(self,id):
        form_url = self.api_url + '/harvestingjob'
        data = {
            'source_id': id,
            'user_ref': ''
        }
        data = json.dumps(data)
        try:
            r = self._do_request(form_url,data)

            h.flash_success('Refresh requested, harvesting will take place within 15 minutes.')
        except urllib2.HTTPError as e:
            msg = 'An error occurred: [%s %s]' % (str(e.getcode()),e.msg)
            if e.getcode() == 400:
                msg = msg + ' ' + e.read()
                
            h.flash_error(msg) 
        finally:
            redirect(h.url_for(controller='harvest', action='index', id=None))

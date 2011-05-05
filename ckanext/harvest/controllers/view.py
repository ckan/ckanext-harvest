import urllib2

from pylons.i18n import _

import ckan.lib.helpers as h, json
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort, redirect

from ckan.model import Package

from ckanext.harvest.lib import *

class ViewController(BaseController):

    api_url = config.get('ckan.api_url', 'http://localhost:5000').rstrip('/')+'/api/2/rest'
    form_api_url = config.get('ckan.api_url', 'http://localhost:5000').rstrip('/')+'/api/2/form'
    api_key = config.get('ckan.harvest.api_key')

    def __before__(self, action, **env):
        super(ViewController, self).__before__(action, **env)
        # All calls to this controller must be with a sysadmin key
        if not self.authorizer.is_sysadmin(c.user):
            response_msg = _('Not authorized to see this page')
            status = 401
            abort(status, response_msg)

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
        # Request all harvest sources
        c.sources = get_harvest_sources()

        return render('ckanext/harvest/index.html')

    def create(self):

        # This is the DGU form API, so we don't use self.api_url
        form_url = self.form_api_url + '/harvestsource/create'
        if request.method == 'GET':
            try:
                # Request the fields
                c.form = self._do_request(form_url).read()
                c.mode = 'create'
            except urllib2.HTTPError as e:
                msg = 'An error occurred: [%s %s]' % (str(e.getcode()),e.msg)
                h.flash_error(msg)
            return render('ckanext/harvest/create.html')
        if request.method == 'POST':
            # Build an object like the one expected by the DGU form API
            data = {
                'form_data':
                    {'HarvestSource--url': request.POST['HarvestSource--url'],
                     'HarvestSource--description': request.POST['HarvestSource--description'],
                     'HarvestSource--type': request.POST['HarvestSource--type'],
                    },
                'user_id':'',
                'publisher_id':''
            }
            data = json.dumps(data)
            try:
                rq = self._do_request(form_url,data)

                h.flash_success('Harvesting source added successfully')
                redirect(h.url_for('harvest'))

            except urllib2.HTTPError as e:
                msg = 'An error occurred: [%s %s]' % (str(e.getcode()),e.msg)
                # The form API returns just a 500, so we are not exactly sure of what
                # happened, but most probably it was a duplicate entry
                if e.getcode() == 500:
                    msg = msg + ' Does the source already exist?'
                elif e.getcode() == 400:
                    err_msg = e.read()
                    if '<form' in c.form:
                        c.form = err_msg
                        c.mode = 'create'
                        return render('ckanext/harvest/create.html')
                    else:
                        msg = err_msg

                h.flash_error(msg)
                redirect(h.url_for('harvest'))

    def show(self,id):
        try:
            c.source = get_harvest_source(id)

            return render('ckanext/harvest/show.html')
        except:
            abort(404,'Harvest source not found')


    def delete(self,id):
        try:
            delete_harvest_source(id)
            h.flash_success('Harvesting source deleted successfully')
        except Exception as e:
            msg = 'An error occurred: [%s]' % e.message
            h.flash_error(msg)

        redirect(h.url_for('harvest'))

    def edit(self,id):

        form_url = self.form_api_url + '/harvestsource/edit/%s' % id
        if request.method == 'GET':
            # Request the fields
            c.form = self._do_request(form_url).read()
            c.mode = 'edit'

            return render('ckanext/harvest/create.html')
        if request.method == 'POST':
            # Build an object like the one expected by the DGU form API
            data = {
                'form_data':
                    {'HarvestSource-%s-url' % id: request.POST['HarvestSource-%s-url' % id] ,
                     'HarvestSource-%s-type' % id: request.POST['HarvestSource-%s-type' % id],
                     'HarvestSource-%s-description' % id: request.POST['HarvestSource-%s-description' % id]},
                'user_id':'',
                'publisher_id':''
            }
            data = json.dumps(data)
            try:
                r = self._do_request(form_url,data)

                h.flash_success('Harvesting source edited successfully')

                redirect(h.url_for('harvest'))
            except urllib2.HTTPError as e:
                if e.getcode() == 400:
                    c.form = e.read()
                    c.mode = 'edit'
                    return render('ckanext/harvest/create.html')
                else:
                    msg = 'An error occurred: [%s %s]' % (str(e.getcode()),e.msg)
                    h.flash_error(msg)
                    redirect(h.url_for('harvest'))

    def create_harvesting_job(self,id):
        try:
            create_harvest_job(id)
            h.flash_success('Refresh requested, harvesting will take place within 15 minutes.')
        except Exception as e:
            msg = 'An error occurred: [%s]' % e.message
            h.flash_error(msg)

        redirect(h.url_for('harvest'))


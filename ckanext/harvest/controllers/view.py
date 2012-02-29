from lxml import etree
from lxml.etree import XMLSyntaxError
from pylons.i18n import _

from ckan import model

import ckan.lib.helpers as h, json
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort, redirect

from ckan.lib.navl.dictization_functions import DataError
from ckan.logic import NotFound, ValidationError, get_action
from ckanext.harvest.logic.schema import harvest_source_form_schema
from ckanext.harvest.lib import create_harvest_source, edit_harvest_source, \
                                create_harvest_job, get_registered_harvesters_info, \
                                get_harvest_object
from ckan.lib.helpers import Page
import logging
log = logging.getLogger(__name__)

class ViewController(BaseController):

    def __before__(self, action, **env):
        super(ViewController, self).__before__(action, **env)
        # All calls to this controller must be with a sysadmin key
        if not self.authorizer.is_sysadmin(c.user):
            response_msg = _('Not authorized to see this page')
            status = 401
            abort(status, response_msg)

    def index(self):
        # Request all harvest sources
        context = {'model':model}

        c.sources = get_action('harvest_source_list')(context,{})

        return render('index.html')

    def new(self,data = None,errors = None, error_summary = None):

        if ('save' in request.params) and not data:
            return self._save_new()

        data = data or {}
        errors = errors or {}
        error_summary = error_summary or {}
        vars = {'data': data, 'errors': errors, 'error_summary': error_summary, 'harvesters': get_registered_harvesters_info()}

        c.form = render('source/new_source_form.html', extra_vars=vars)
        return render('source/new.html')

    def _save_new(self):
        try:
            data_dict = dict(request.params)
            self._check_data_dict(data_dict)

            source = create_harvest_source(data_dict)

            # Create a harvest job for the new source
            create_harvest_job(source['id'])

            h.flash_success(_('New harvest source added successfully.'
                    'A new harvest job for the source has also been created.'))
            redirect(h.url_for('harvest'))
        except DataError,e:
            abort(400, 'Integrity Error')
        except ValidationError,e:
            errors = e.error_dict
            error_summary = e.error_summary if hasattr(e,'error_summary') else None
            return self.new(data_dict, errors, error_summary)

    def edit(self, id, data = None,errors = None, error_summary = None):

        if ('save' in request.params) and not data:
            return self._save_edit(id)


        if not data:
            try:
                context = {'model':model}

                old_data = get_action('harvest_source_show')(context, {'id':id})
            except NotFound:
                abort(404, _('Harvest Source not found'))

        data = data or old_data
        errors = errors or {}
        error_summary = error_summary or {}

        vars = {'data': data, 'errors': errors, 'error_summary': error_summary, 'harvesters': get_registered_harvesters_info()}

        c.form = render('source/new_source_form.html', extra_vars=vars)
        return render('source/edit.html')

    def _save_edit(self,id):
        try:
            data_dict = dict(request.params)
            self._check_data_dict(data_dict)

            source = edit_harvest_source(id,data_dict)

            h.flash_success(_('Harvest source edited successfully.'))
            redirect(h.url_for('harvest'))
        except DataError,e:
            abort(400, _('Integrity Error'))
        except NotFound, e:
            abort(404, _('Harvest Source not found'))
        except ValidationError,e:
            errors = e.error_dict
            error_summary = e.error_summary if hasattr(e,'error_summary') else None
            return self.edit(id,data_dict, errors, error_summary)

    def _check_data_dict(self, data_dict):
        '''Check if the return data is correct'''
        surplus_keys_schema = ['id','publisher_id','user_id','active','save','config']

        schema_keys = harvest_source_form_schema().keys()
        keys_in_schema = set(schema_keys) - set(surplus_keys_schema)

        if keys_in_schema - set(data_dict.keys()):
            log.info(_('Incorrect form fields posted'))
            raise DataError(data_dict)

    def read(self,id):
        try:
            context = {'model':model}
            c.source = get_action('harvest_source_show')(context, {'id':id})

            c.page = Page(
                collection=c.source['status']['packages'],
                page=request.params.get('page', 1),
                items_per_page=20
            )

            return render('source/read.html')
        except NotFound:
            abort(404,_('Harvest source not found'))


    def delete(self,id):
        try:
            delete_harvest_source(id)

            h.flash_success(_('Harvesting source deleted successfully'))
            redirect(h.url_for('harvest'))
        except NotFound:
            abort(404,_('Harvest source not found'))


    def create_harvesting_job(self,id):
        try:
            create_harvest_job(id)
            h.flash_success(_('Refresh requested, harvesting will take place within 15 minutes.'))
        except NotFound:
            abort(404,_('Harvest source not found'))
        except Exception, e:
            msg = 'An error occurred: [%s]' % e.message
            h.flash_error(msg)

        redirect(h.url_for('harvest'))

    def show_object(self,id):
        try:
            context = {'model':model}
            obj = get_action('harvest_object_show')(context, {'id':id})

            # Check content type. It will probably be either XML or JSON
            try:
                etree.fromstring(obj['content'])
                response.content_type = 'application/xml'
            except XMLSyntaxError:
                try:
                    json.loads(obj['content'])
                    response.content_type = 'application/json'
                except ValueError:
                    pass

            response.headers['Content-Length'] = len(obj['content'])
            return obj['content']
        except NotFound:
            abort(404,_('Harvest object not found'))
        except Exception, e:
            msg = 'An error occurred: [%s]' % e.message
            h.flash_error(msg)

        redirect(h.url_for('harvest'))


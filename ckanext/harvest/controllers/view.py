import re
import json
from lxml import etree
from lxml.etree import XMLSyntaxError
from pylons.i18n import _

from ckan.new_authz import is_sysadmin
from ckan import model, plugins as p
from ckan.model.group import Group

import ckan.lib.helpers as h
from ckan.lib.base import (BaseController, c, request,
                          response, config, abort, redirect)

from ckan.lib.navl.dictization_functions import DataError
from ckan.logic import NotFound, ValidationError, get_action, NotAuthorized
from ckanext.harvest.logic.schema import harvest_source_form_schema

from ckan.lib.helpers import Page, pager_url

import logging
log = logging.getLogger(__name__)

class ViewController(BaseController):

    not_auth_message = _('Not authorized to see this page')

    def __before__(self, action, **params):

        super(ViewController, self).__before__(action, **params)

        c.publisher_auth = (config.get('ckan.harvest.auth.profile', None) == 'publisher')

    def _get_publishers(self):
        groups = []

        if c.publisher_auth:
            if is_sysadmin(c.user):
                groups = Group.all(group_type='publisher')
            elif c.userobj:
                groups = c.userobj.get_groups('publisher')
            else:  # anonymous user shouldn't have access to this page anyway.
                groups = []

            # Be explicit about which fields we make available in the template
            groups = [{
                'name': g.name,
                'id': g.id,
                'title': g.title
            } for g in groups]

        return groups


    def index(self):
        context = {'model':model, 'user':c.user,'session':model.Session}
        try:
            # Request all harvest sources
            c.sources = get_action('harvest_source_list')(context,{})
        except NotAuthorized,e:
            abort(401,self.not_auth_message)

        if c.publisher_auth:
            c.sources = sorted(c.sources,key=lambda source : source['publisher_title'])

        c.status = config.get('ckan.harvest.status')

        return p.toolkit.render('ckanext/harvest/index.html')

    def new(self,data = None,errors = None, error_summary = None):

        if ('save' in request.params) and not data:
            return self._save_new()

        data = data or {}
        errors = errors or {}
        error_summary = error_summary or {}
        context = {'model': model, 'user': c.user}

        try:
            harvesters_info = get_action('harvesters_info_show')(context, {})
        except NotAuthorized:
            abort(401, self.not_auth_message)

        template_vars = {
            'data': data,
            'errors': errors,
            'error_summary': error_summary,
            'harvesters': harvesters_info
        }

        c.groups = self._get_publishers()
        return p.toolkit.render('ckanext/harvest/source/new.html', template_vars)

    def _save_new(self):
        try:
            data_dict = dict(request.params)
            self._check_data_dict(data_dict)
            context = {'model':model, 'user':c.user, 'session':model.Session,
                       'schema':harvest_source_form_schema()}

            source = get_action('harvest_source_create')(context,data_dict)

            # Create a harvest job for the new source
            get_action('harvest_job_create')(context,{'source_id':source['id']})

            h.flash_success(_('New harvest source added successfully.'
                    'A new harvest job for the source has also been created.'))
            redirect('/harvest/%s' % source['id'])
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
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
                context = {'model':model, 'user':c.user}

                old_data = get_action('harvest_source_show')(context, {'id':id})
            except NotFound:
                abort(404, _('Harvest Source not found'))
            except NotAuthorized,e:
                abort(401,self.not_auth_message)

        data = data or old_data
        errors = errors or {}
        error_summary = error_summary or {}
        try:
            context = {'model':model, 'user':c.user}
            harvesters_info = get_action('harvesters_info_show')(context,{})
        except NotAuthorized,e:
            abort(401,self.not_auth_message)

        template_vars = {'data': data, 'errors': errors, 'error_summary': error_summary, 'harvesters': harvesters_info}

        c.groups = self._get_publishers()
        return p.toolkit.render('ckanext/harvest/source/edit.html', template_vars)

    def _save_edit(self,id):
        try:
            data_dict = dict(request.params)
            data_dict['id'] = id
            self._check_data_dict(data_dict)
            context = {'model':model, 'user':c.user, 'session':model.Session,
                       'schema':harvest_source_form_schema()}

            source = get_action('harvest_source_update')(context,data_dict)

            h.flash_success(_('Harvest source edited successfully.'))
            redirect('/harvest/%s' %id)
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
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
        surplus_keys_schema = ['id','publisher_id','user_id','config','save']
        schema_keys = harvest_source_form_schema().keys()
        keys_in_schema = set(schema_keys) - set(surplus_keys_schema)

        # user_id is not yet used, we'll set the logged user one for the time being
        if not data_dict.get('user_id',None):
            if c.userobj:
                data_dict['user_id'] = c.userobj.id
        if keys_in_schema - set(data_dict.keys()):
            log.info(_('Incorrect form fields posted'))
            raise DataError(data_dict)

    def read(self, id):
        try:
            context = {'model':model, 'user':c.user}
            c.source = get_action('harvest_source_show')(context, {'id':id})

            c.page = Page(
                collection=c.source['status']['packages'],
                page=request.params.get('page', 1),
                items_per_page=20,
                url=pager_url
            )
            return p.toolkit.render('ckanext/harvest/source/read.html')
        except NotFound:
            abort(404,_('Harvest source not found'))
        except NotAuthorized,e:
            abort(401,self.not_auth_message)



    def delete(self,id):
        try:
            context = {'model':model, 'user':c.user}
            get_action('harvest_source_delete')(context, {'id':id})

            h.flash_success(_('Harvesting source successfully inactivated'))
            redirect(h.url_for('harvest'))
        except NotFound:
            abort(404,_('Harvest source not found'))
        except NotAuthorized,e:
            abort(401,self.not_auth_message)


    def create_harvesting_job(self,id):
        try:
            context = {'model':model, 'user':c.user, 'session':model.Session}
            get_action('harvest_job_create')(context,{'source_id':id})
            h.flash_success(_('Refresh requested, harvesting will take place within 15 minutes.'))
        except NotFound:
            abort(404,_('Harvest source not found'))
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
        except Exception, e:
            msg = 'An error occurred: [%s]' % e.message
            h.flash_error(msg)

        redirect(h.url_for('harvest'))

    def show_object(self,id):

        try:
            context = {'model':model, 'user':c.user}
            obj = get_action('harvest_object_show')(context, {'id':id})

            # Check content type. It will probably be either XML or JSON
            try:
                content = re.sub('<\?xml(.*)\?>','', obj['content'])
                etree.fromstring(content)
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
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
        except Exception, e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500,msg)

import re
from lxml import etree
from lxml.etree import XMLSyntaxError
from pylons.i18n import _

from ckan import model
from ckan.model.group import Group

import ckan.lib.helpers as h, json
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort, redirect

from ckan.lib.navl.dictization_functions import DataError
from ckan.logic import NotFound, ValidationError, get_action, NotAuthorized
from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.logic.schema import harvest_source_form_to_db_schema

from ckan.lib.helpers import Page,pager_url
import ckan.plugins as p

import logging
log = logging.getLogger(__name__)

class ViewController(BaseController):

    not_auth_message = p.toolkit._('Not authorized to see this page')

    def __before__(self, action, **params):

        super(ViewController,self).__before__(action, **params)

        #TODO: remove
        c.publisher_auth = (config.get('ckan.harvest.auth.profile',None) == 'publisher')

        c.dataset_type = DATASET_TYPE_NAME

    def _get_publishers(self):
        groups = None
        user = model.User.get(c.user)
        if c.publisher_auth:
            if user.sysadmin:
                groups = Group.all(group_type='publisher')
            elif c.userobj:
                groups = c.userobj.get_groups('publisher')
            else: # anonymous user shouldn't have access to this page anyway.
                groups = []

            # Be explicit about which fields we make available in the template
            groups = [ {
                'name': g.name,
                'id': g.id,
                'title': g.title,
            } for g in groups ]

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

        return render('index.html')

    def new(self,data = None,errors = None, error_summary = None):

        if ('save' in request.params) and not data:
            return self._save_new()

        data = data or {}
        errors = errors or {}
        error_summary = error_summary or {}

        try:
            context = {'model':model, 'user':c.user}
            harvesters_info = get_action('harvesters_info_show')(context,{})
        except NotAuthorized,e:
            abort(401,self.not_auth_message)

        vars = {'data': data, 'errors': errors, 'error_summary': error_summary, 'harvesters': harvesters_info}

        c.groups = self._get_publishers()

        vars['form_items'] = self._make_autoform_items(harvesters_info)

        c.form = render('source/old_new_source_form.html', extra_vars=vars)
        return render('source/new.html')



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

        vars = {'data': data, 'errors': errors, 'error_summary': error_summary, 'harvesters': harvesters_info}

        c.groups = self._get_publishers()

        vars['form_items'] = self._make_autoform_items(harvesters_info)

        c.form = render('source/old_new_source_form.html', extra_vars=vars)

        return render('source/edit.html')

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

        # TODO: remove frequency once it is added to the frontend!
        surplus_keys_schema = ['id','publisher_id','user_id','config','save','frequency']
        schema_keys = harvest_source_form_to_db_schema().keys()
        keys_in_schema = set(schema_keys) - set(surplus_keys_schema)

        # user_id is not yet used, we'll set the logged user one for the time being
        if not data_dict.get('user_id',None):
            if c.userobj:
                data_dict['user_id'] = c.userobj.id
        if keys_in_schema - set(data_dict.keys()):
            log.info(_('Incorrect form fields posted'))
            raise DataError(data_dict)

    def read(self,id):
        try:
            context = {'model':model, 'user':c.user}
            c.source = get_action('harvest_source_show')(context, {'id':id})
            c.page = Page(
                collection=c.source['status']['packages'],
                page=request.params.get('page', 1),
                items_per_page=20,
                url=pager_url
            )

            return render('source/read.html')
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
            if 'Can not create jobs on inactive sources' in str(e):
                h.flash_error(_('Cannot create new harvest jobs on inactive sources.'
                                 + ' First, please change the source status to \'active\'.'))
            elif 'There already is an unrun job for this source' in str(e):
                h.flash_notice(_('A harvest job has already been scheduled for this source'))
            else:
                msg = 'An error occurred: [%s]' % str(e)
                h.flash_error(msg)

        redirect(h.url_for('harvest'))

    def show_object(self,id):

        try:
            context = {'model':model, 'user':c.user}
            obj = get_action('harvest_object_show')(context, {'id':id})

            # Check content type. It will probably be either XML or JSON
            try:

                if obj['content']:
                    content = obj['content']
                elif 'original_document' in obj['extras']:
                    content = obj['extras']['original_document']
                else:
                    abort(404,_('No content found'))

                etree.fromstring(re.sub('<\?xml(.*)\?>','',content))
                response.content_type = 'application/xml; charset=utf-8'
                if not '<?xml' in content.split('\n')[0]:
                    content = u'<?xml version="1.0" encoding="UTF-8"?>\n' + content

            except XMLSyntaxError:
                try:
                    json.loads(obj['content'])
                    response.content_type = 'application/json; charset=utf-8'
                except ValueError:
                    # Just return whatever it is
                    pass

            response.headers['Content-Length'] = len(content)
            return content.encode('utf-8')
        except NotFound:
            abort(404,_('Harvest object not found'))
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
        except Exception, e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500,msg)


    def _get_source_for_job(self, source_id):

        try:
            context = {'model': model, 'user': c.user}
            source_dict = p.toolkit.get_action('harvest_source_show')(context,
                    {'id': source_id})
        except NotFound:
            abort(404, p.toolkit._('Harvest source not found'))
        except NotAuthorized,e:

            abort(401,self.not_auth_message)
        except Exception, e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500,msg)

        return source_dict

    def show_job(self, id, source_dict=False, is_last=False):

        try:
            context = {'model':model, 'user':c.user}
            c.job = get_action('harvest_job_show')(context, {'id': id})
            c.job_report = get_action('harvest_job_report')(context, {'id': id})

            if not source_dict:
                source_dict = get_action('harvest_source_show')(context, {'id': c.job['source_id']})

            c.harvest_source = source_dict
            c.is_last_job = is_last

            return render('job/read.html')

        except NotFound:
            abort(404,_('Harvest job not found'))
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
        except Exception, e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500,msg)


    def show_last_job(self, source):

        source_dict = self._get_source_for_job(source)

        return self.show_job(source_dict['status']['last_job']['id'],
                             source_dict=source_dict,
                             is_last=True)


    def list_jobs(self, source):

        try:
            context = {'model':model, 'user':c.user}
            c.harvest_source =  get_action('harvest_source_show')(context, {'id': source})
            c.jobs = get_action('harvest_job_list')(context, {'source_id': c.harvest_source['id']})

            return render('job/list.html')

        except NotFound:
            abort(404,_('Harvest source not found'))
        except NotAuthorized,e:
            abort(401,self.not_auth_message)
        except Exception, e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500,msg)


    def _make_autoform_items(self, harvesters_info):
        states = [{'text': 'active', 'value': 'True'},
                  {'text': 'withdrawn', 'value': 'False'},]

        harvest_list = []
        harvest_descriptions = p.toolkit.literal('<ul>')
        for harvester in harvesters_info:
            harvest_list.append({'text':harvester['title'], 'value': harvester['name']})
            harvest_descriptions += p.toolkit.literal('<li><span class="harvester-title">')
            harvest_descriptions += harvester['title']
            harvest_descriptions += p.toolkit.literal('</span>: ')
            harvest_descriptions += harvester['description']
            harvest_descriptions += p.toolkit.literal('</li>')
        harvest_descriptions += p.toolkit.literal('</ul>')

        items = [
            {'name': 'url', 'control': 'input', 'label': _('URL'), 'placeholder': _(''), 'extra_info': 'This should include the http:// part of the URL'},
            {'name': 'type', 'control': 'select', 'options': harvest_list, 'label': _('Source type'), 'placeholder': _(''), 'extra_info': 'Which type of source does the URL above represent? '},
            {'control': 'html', 'html': harvest_descriptions},
            {'name': 'title', 'control': 'input', 'label': _('Title'), 'placeholder': _(''), 'extra_info': 'This will be shown as the datasets source.'},
            {'name': 'description', 'control': 'textarea', 'label': _('Description'), 'placeholder': _(''), 'extra_info':'You can add your own notes here about what the URL above represents to remind you later.'},]

        if c.groups:
            pubs = []
            for group in c.groups:
                pubs.append({'text':group['title'], 'value': group['id']})
            items.append({'name': 'publisher_id', 'control': 'select', 'options': pubs, 'label': _('Publisher'), 'placeholder': _('')})

        items += [
            {'name': 'config', 'control': 'textarea', 'label': _('Configuration'), 'placeholder': _(''), 'extra_info': ''},
            {'name': 'active', 'control': 'select', 'options': states, 'label': _('State'), 'placeholder': _(''), 'extra_text': ''},
        ]

        return items

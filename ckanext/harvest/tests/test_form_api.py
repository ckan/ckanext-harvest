# NB These tests have been copied as bleeding chunks from
# dgu/ckanext/dgu/tests/forms/test_form_api.py and will need fixing up
# drastically.

from ckan.tests import *
from ckan.tests import search_related
from ckan.tests.functional.api.base import (ApiTestCase,
                                            Api1TestCase,
                                            Api2TestCase,
                                            ApiUnversionedTestCase)
from ckanext.dgu.tests import WsgiAppCase, MockDrupalCase

from ckanext.harvest.lib import get_harvest_source, create_harvest_source

import ckan.model as model
import ckan.authz as authz

class BaseFormsApiCase(ModelMethods, ApiTestCase, WsgiAppCase, CommonFixtureMethods, CheckMethods, MockDrupalCase):
    '''Utilities and pythonic wrapper for the Forms API for testing it.'''
    @staticmethod
    def get_harvest_source_by_url(source_url, default=Exception):
        return get_harvest_source(source_url,attr='url',default=default)

    def create_harvest_source(self, **kwds):
        source = create_harvest_source(kwds)
        return source
   
    def delete_harvest_source(self, url):
        source = self.get_harvest_source_by_url(url, None)
        if source:
            self.delete_commit(source)

    def offset_harvest_source_create_form(self):
        return self.offset('/form/harvestsource/create')

    def offset_harvest_source_edit_form(self, ref):
        return self.offset('/form/harvestsource/edit/%s' % ref)

    def get_harvest_source_create_form(self, status=[200]):
        offset = self.offset_harvest_source_create_form()
        res = self.get(offset, status=status)
        return self.form_from_res(res)

    def get_harvest_source_edit_form(self, harvest_source_id, status=[200]):
        offset = self.offset_harvest_source_edit_form(harvest_source_id)
        res = self.get(offset, status=status)
        return self.form_from_res(res)

    def post_harvest_source_create_form(self, form=None, status=[201], **field_args):
        if form == None:
            form = self.get_harvest_source_create_form()
        for key,field_value in field_args.items():
            field_name = 'HarvestSource--%s' % key
            form[field_name] = field_value
        form_data = form.submit_fields()
        data = {
            'form_data': form_data,
            'user_id': 'example publisher user',
            'publisher_id': 'example publisher',
        }
        offset = self.offset_harvest_source_create_form()
        return self.post(offset, data, status=status)

    def post_harvest_source_edit_form(self, harvest_source_id, form=None, status=[200], **field_args):
        if form == None:
            form = self.get_harvest_source_edit_form(harvest_source_id)
        for key,field_value in field_args.items():
            field_name = 'HarvestSource-%s-%s' % (harvest_source_id, key)
            self.set_formfield(form, field_name, field_value)
        form_data = form.submit_fields()
        data = {
            'form_data': form_data,
            'user_id': 'example publisher user',
            'publisher_id': 'example publisher',
        }
        offset = self.offset_harvest_source_edit_form(harvest_source_id)
        return self.post(offset, data, status=status)
        

class FormsApiTestCase(BaseFormsApiCase):

    @classmethod
    def setup_class(cls):
        super(FormsApiTestCase, cls).setup_class()
        from ckanext.harvest.model import setup as harvest_setup
        harvest_setup()

    def setup(self):
        model.repo.init_db()
        CreateTestData.create()
        self.package_name = u'formsapi'
        self.package_name_alt = u'formsapialt'
        self.package_name_alt2 = u'formsapialt2'
        self.apikey_header_name = config.get('apikey_header_name', 'X-CKAN-API-Key')

        self.user = self.get_user_by_name(u'tester')
        if not self.user:
            self.user = self.create_user(name=u'tester')
        self.user = self.get_user_by_name(u'tester')
        model.add_user_to_role(self.user, model.Role.ADMIN, model.System())
        model.repo.commit_and_remove()
        self.extra_environ = {
            self.apikey_header_name : str(self.user.apikey)
        }
        self.create_package(name=self.package_name)
        self.harvest_source = None

    def teardown(self):
        model.repo.rebuild_db()
        model.Session.connection().invalidate()

    @classmethod
    def teardown_class(cls):
        super(FormsApiTestCase, cls).teardown_class()        

    def test_get_harvest_source_create_form(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        form = self.get_harvest_source_create_form()
        self.assert_formfield(form, 'HarvestSource--url', '')
        self.assert_formfield(form, 'HarvestSource--type', 'CSW Server')
        self.assert_formfield(form, 'HarvestSource--description', '')

    def test_submit_harvest_source_create_form_valid(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        source_url = u'http://localhost/'
        source_type= u'CSW Server'
        source_description = u'My harvest source.'
        assert not self.get_harvest_source_by_url(source_url, None)
        res = self.post_harvest_source_create_form(url=source_url,type=source_type,description=source_description)
        self.assert_header(res, 'Location')
        # Todo: Check the Location looks promising (extract and check given ID).
        self.assert_blank_response(res)
        source = self.get_harvest_source_by_url(source_url) # Todo: Use extracted ID.
        assert_equal(source['user_id'], 'example publisher user')
        assert_equal(source['publisher_id'], 'example publisher')

    def test_submit_harvest_source_create_form_invalid(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        source_url = u'' # Blank URL.
        source_type= u'CSW Server'
        assert not self.get_harvest_source_by_url(source_url, None)
        res = self.post_harvest_source_create_form(url=source_url, status=[400])
        self.assert_not_header(res, 'Location')
        assert "URL for source of metadata: Please enter a value" in res.body, res.body
        assert not self.get_harvest_source_by_url(source_url, None)

        source_url = u'something' # Not '^http://'
        source_type= u'CSW Server'
        assert not self.get_harvest_source_by_url(source_url, None)
        res = self.post_harvest_source_create_form(url=source_url, status=[400])
        self.assert_not_header(res, 'Location')
        assert "URL for source of metadata: Harvest source URL is invalid" in res.body, res.body
        assert not self.get_harvest_source_by_url(source_url, None)


    def test_get_harvest_source_edit_form(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        source_url = u'http://'
        source_type = u'CSW Server'
        source_description = u'An example harvest source.'
        self.harvest_source = self.create_harvest_source(url=source_url,type=source_type,description=source_description)
        form = self.get_harvest_source_edit_form(self.harvest_source['id'])
        self.assert_formfield(form, 'HarvestSource-%s-url' % self.harvest_source['id'], source_url)
        self.assert_formfield(form, 'HarvestSource-%s-type' % self.harvest_source['id'], source_type)
        self.assert_formfield(form, 'HarvestSource-%s-description' % self.harvest_source['id'], source_description)
 
    def test_submit_harvest_source_edit_form_valid(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        source_url = u'http://'
        source_type = u'CSW Server'
        source_description = u'An example harvest source.'
        alt_source_url = u'http://a'
        alt_source_type = u'Web Accessible Folder (WAF)'
        alt_source_description = u'An old example harvest source.'
        self.harvest_source = self.create_harvest_source(url=source_url, type=source_type,description=source_description)
        assert self.get_harvest_source_by_url(source_url, None)
        assert not self.get_harvest_source_by_url(alt_source_url, None)
        res = self.post_harvest_source_edit_form(self.harvest_source['id'], url=alt_source_url, type=alt_source_type,description=alt_source_description)
        self.assert_not_header(res, 'Location')
        # Todo: Check the Location looks promising (extract and check given ID).
        self.assert_blank_response(res)
        assert not self.get_harvest_source_by_url(source_url, None)
        source = self.get_harvest_source_by_url(alt_source_url) # Todo: Use extracted ID.
        assert source
        assert_equal(source['user_id'], 'example publisher user')
        assert_equal(source['publisher_id'], 'example publisher')

    def test_submit_harvest_source_edit_form_invalid(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        source_url = u'http://'
        source_type = u'CSW Server'
        source_description = u'An example harvest source.'
        alt_source_url = u''
        self.harvest_source = self.create_harvest_source(url=source_url, type=source_type,description=source_description)
        assert self.get_harvest_source_by_url(source_url, None)
        res = self.post_harvest_source_edit_form(self.harvest_source['id'], url=alt_source_url, status=[400])
        assert self.get_harvest_source_by_url(source_url, None)
        self.assert_not_header(res, 'Location')
        assert "URL for source of metadata: Please enter a value" in res.body, res.body

class FormsApiAuthzTestCase(BaseFormsApiCase):
    def setup(self):
        # need to do this for every test since we mess with System rights
        CreateTestData.create()
        model.repo.new_revision()
        model.Session.add(model.User(name=u'testadmin'))
        model.Session.add(model.User(name=u'testsysadmin'))
        model.Session.add(model.User(name=u'notadmin'))
        model.repo.commit_and_remove()

        pkg = model.Package.by_name(u'annakarenina')
        admin = model.User.by_name(u'testadmin')
        sysadmin = model.User.by_name(u'testsysadmin')
        model.add_user_to_role(admin, model.Role.ADMIN, pkg)
        model.add_user_to_role(sysadmin, model.Role.ADMIN, model.System())
        model.repo.commit_and_remove()

        self.pkg = model.Package.by_name(u'annakarenina')
        self.admin = model.User.by_name(u'testadmin')
        self.sysadmin = model.User.by_name(u'testsysadmin')
        self.notadmin = model.User.by_name(u'notadmin')

    def teardown(self):
        model.Session.remove()
        model.repo.rebuild_db()
        model.Session.remove()

    def check_create_harvest_source(self, username, expect_success=True):
        user = model.User.by_name(username)
        self.extra_environ={'Authorization' : str(user.apikey)}
        expect_status = 200 if expect_success else 403
        
        form = self.get_harvest_source_create_form(status=expect_status)

    def check_edit_harvest_source(self, username, expect_success=True):
        # create a harvest source
        source_url = u'http://localhost/'
        source_type = u'CSW Server'
        source_description = u'My harvest source.'
        sysadmin = model.User.by_name(u'testsysadmin')
        self.extra_environ={'Authorization' : str(sysadmin.apikey)}
        if not self.get_harvest_source_by_url(source_url, None):
            res = self.post_harvest_source_create_form(url=source_url, type=source_type,description=source_description)
        harvest_source = self.get_harvest_source_by_url(source_url, None)
        assert harvest_source

        user = model.User.by_name(username)
        self.extra_environ={'Authorization' : str(user.apikey)}
        expect_status = 200 if expect_success else 403
        
        form = self.get_harvest_source_edit_form(harvest_source['id'], status=expect_status)

    def remove_default_rights(self):
        roles = []
        system_role_query = model.Session.query(model.SystemRole)
        package_role_query = model.Session.query(model.PackageRole)
        for pseudo_user in (u'logged_in', u'visitor'):
            roles.extend(system_role_query.join('user').\
                         filter_by(name=pseudo_user).all())
            roles.extend(package_role_query.join('package').\
                         filter_by(name='annakarenina').\
                         join('user').filter_by(name=pseudo_user).all())
        for role in roles:
            role.delete()
        model.repo.commit_and_remove()
        
    def test_harvest_source_create(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        self.check_create_harvest_source('testsysadmin', expect_success=True)
        self.check_create_harvest_source('testadmin', expect_success=False)
        self.check_create_harvest_source('notadmin', expect_success=False)
        self.remove_default_rights()
        self.check_create_harvest_source('testsysadmin', expect_success=True)
        self.check_create_harvest_source('testadmin', expect_success=False)
        self.check_create_harvest_source('notadmin', expect_success=False)

    def test_harvest_source_edit(self):
        raise SkipTest('These tests should be moved to ckanext-harvest.')

        self.check_edit_harvest_source('testsysadmin', expect_success=True)
        self.check_edit_harvest_source('testadmin', expect_success=False)
        self.check_edit_harvest_source('notadmin', expect_success=False)
        self.remove_default_rights()
        self.check_edit_harvest_source('testsysadmin', expect_success=True)
        self.check_edit_harvest_source('testadmin', expect_success=False)
        self.check_edit_harvest_source('notadmin', expect_success=False)

class TestFormsApi1(Api1TestCase, FormsApiTestCase): pass

class TestFormsApi2(Api2TestCase, FormsApiTestCase): pass

class TestFormsApiUnversioned(ApiUnversionedTestCase, FormsApiTestCase): pass

class WithOrigKeyHeader(FormsApiTestCase):
    apikey_header_name = 'Authorization'

class TestFormsApiAuthz1(Api1TestCase, FormsApiAuthzTestCase): pass

class TestFormsApiAuthz2(Api2TestCase, FormsApiAuthzTestCase): pass

class TestFormsApiAuthzUnversioned(ApiUnversionedTestCase, FormsApiAuthzTestCase): pass

from pylons import config

from ckan import model
from ckan.tests import WsgiAppCase, CkanServerCase, url_for
from ckan.tests.html_check import HtmlCheckMethods
from ckan.lib.create_test_data import CreateTestData

class TestHarvest(WsgiAppCase, HtmlCheckMethods, CkanServerCase):
    @classmethod
    def setup_class(cls):
        cls.view_controller = 'ckanext.harvest.controllers.view:ViewController'
        # create sysadmin user with apikey matching that in test-core.ini
        rev = model.repo.new_revision() 
        model.Session.add(model.User(name=u'tester', apikey=u'testkey'))
        model.repo.commit_and_remove()
        rev = model.repo.new_revision() 
        tester = model.User.by_name(u'testsysadmin')
        model.add_user_to_role(model.User.by_name(u'tester'),
                               model.Role.ADMIN, model.System())
        model.repo.commit_and_remove()
        cls.extra_environ = {'REMOTE_USER': 'tester'}

##        cls.ckan_process = cls._start_ckan_server(config['__file__'])
##        cls._wait_for_url()

##    @classmethod
##    def teardown_class(cls):
##        cls._stop_ckan_server(cls.ckan_process)

    def test_not_logged_in(self):
        offset = url_for(controller=self.view_controller,
                         action='index')
        res = self.app.get(offset, status=302)

    def test_harvest_index(self):
        offset = url_for(controller=self.view_controller,
                         action='index',
                         )
        res = self.app.get(offset, extra_environ=self.extra_environ)
        # status is 200 although it displays an error of not being able to
        # access the api.

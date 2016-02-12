from ckan.lib.helpers import url_for

try:
    from ckan.tests import helpers, factories
except ImportError:
    from ckan.new_tests import helpers, factories

from ckanext.harvest.tests import factories as harvest_factories

try:
    from ckan.tests.helpers import assert_in
except ImportError:
    # for ckan 2.2
    try:
        from nose.tools import assert_in
    except ImportError:
        # Python 2.6 doesn't have it
        def assert_in(a, b, msg=None):
            assert a in b, msg or '%r was not in %r' % (a, b)

import ckanext.harvest.model as harvest_model


class TestController(helpers.FunctionalTestBase):

    @classmethod
    def setup_class(cls):
        super(TestController, cls).setup_class()
        harvest_model.setup()
        sysadmin = factories.Sysadmin()
        cls.extra_environ = {'REMOTE_USER': sysadmin['name'].encode('ascii')}

    @classmethod
    def teardown_class(cls):
        super(TestController, cls).teardown_class()
        helpers.reset_db()

    def test_new_form_is_rendered(self):

        url = url_for('harvest_new')

        app = self._get_test_app()

        response = app.get(url, extra_environ=self.extra_environ)

        assert_in('<form id="source-new"', response.body)

    def test_edit_form_is_rendered(self):

        source = harvest_factories.HarvestSource()

        url = url_for('harvest_edit', id=source['id'])

        app = self._get_test_app()

        response = app.get(url, extra_environ=self.extra_environ)

        assert_in('<form id="source-new"', response.body)

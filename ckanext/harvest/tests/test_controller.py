from ckan.lib.helpers import url_for

try:
    from ckan.tests import helpers
except ImportError:
    from ckan.new_tests import helpers

from ckanext.harvest.tests import factories


assert_in = helpers.assert_in


class TestController(helpers.FunctionalTestBase):

    def test_new_form_is_rendered(self):

        url = url_for('harvest_new')

        app = self._get_test_app()

        response = app.get(url)

        assert_in('<form id="source-new"', response.body)

    def test_edit_form_is_rendered(self):

        source = factories.HarvestSource()

        url = url_for('harvest_edit', id=source['id'])

        app = self._get_test_app()

        response = app.get(url)

        assert_in('<form id="source-new"', response.body)

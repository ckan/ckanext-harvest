import six
import pytest

from ckantoolkit import url_for
from ckantoolkit.tests import factories
from ckanext.harvest.tests import factories as harvest_factories


def _assert_in_body(string, response):
    if six.PY2:
        assert string in response.body.decode('utf8')
    else:
        assert string in response.body


@pytest.mark.usefixtures('clean_db', 'clean_index', 'harvest_setup')
class TestBlueprint():

    def setup(self):
        sysadmin = factories.Sysadmin()
        self.extra_environ = {'REMOTE_USER': sysadmin['name'].encode('ascii')}

    def test_index_page_is_rendered(self, app):

        source1 = harvest_factories.HarvestSource()
        source2 = harvest_factories.HarvestSource()

        response = app.get(u'/harvest')

        _assert_in_body(source1['title'], response)
        _assert_in_body(source2['title'], response)

    def test_new_form_is_rendered(self, app):

        url = url_for('harvest_new')

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body('<form id="source-new"', response)

    def test_edit_form_is_rendered(self, app):

        source = harvest_factories.HarvestSource()

        url = url_for('harvest_edit', id=source['id'])

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body('<form id="source-new"', response)

    def test_source_page_rendered(self, app):

        source = harvest_factories.HarvestSource()

        url = url_for('harvest_read', id=source['name'])

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body(source['name'], response)

    def test_admin_page_rendered(self, app):

        source_obj = harvest_factories.HarvestSourceObj()
        job = harvest_factories.HarvestJob(source=source_obj)

        url = url_for('harvest_admin', id=source_obj.id)

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body(source_obj.title, response)

        _assert_in_body(job['id'], response)

    def test_about_page_rendered(self, app):

        source = harvest_factories.HarvestSource()

        url = url_for('harvest_about', id=source['name'])

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body(source['name'], response)

    def test_job_page_rendered(self, app):

        job = harvest_factories.HarvestJob()

        url = url_for('harvest_job_list', source=job['source_id'])

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body(job['id'], response)

    def test_job_show_last_page_rendered(self, app):

        job = harvest_factories.HarvestJob()

        url = url_for('harvest_job_show_last', source=job['source_id'])

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body(job['id'], response)

    def test_job_show_page_rendered(self, app):

        job = harvest_factories.HarvestJob()

        url = url_for(
            'harvest_job_show', source=job['source_id'], id=job['id'])

        response = app.get(url, extra_environ=self.extra_environ)

        _assert_in_body(job['id'], response)

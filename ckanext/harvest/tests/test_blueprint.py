import pytest

from ckantoolkit import url_for
from ckantoolkit.tests import factories
from ckanext.harvest.tests import factories as harvest_factories


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index')
class TestBlueprint():

    def test_index_page_is_rendered(self, app):

        source1 = harvest_factories.HarvestSource()
        source2 = harvest_factories.HarvestSource()

        response = app.get(u'/harvest')

        assert source1['title'] in response.body
        assert source2['title'] in response.body

    def test_new_form_is_rendered(self, app):

        url = url_for('harvest.new')
        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        response = app.get(url, extra_environ=env)

        assert '<form id="source-new"' in response.body

    def test_edit_form_is_rendered(self, app):

        source = harvest_factories.HarvestSource()

        url = url_for('harvest.edit', id=source['id'])
        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        response = app.get(url, extra_environ=env)

        assert '<form id="source-new"' in response.body

    def test_source_page_rendered(self, app):

        source = harvest_factories.HarvestSource()

        url = url_for('harvest.read', id=source['name'])
        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        response = app.get(url, extra_environ=env)

        assert source['name'] in response.body

    def test_admin_page_rendered(self, app):

        source_obj = harvest_factories.HarvestSourceObj()
        job = harvest_factories.HarvestJob(source=source_obj)

        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        url = url_for('harvester.admin', id=source_obj.id)

        response = app.get(url, extra_environ=env)

        assert source_obj.title in response.body

        assert job['id'] in response.body

    def test_about_page_rendered(self, app):

        source = harvest_factories.HarvestSource()

        url = url_for('harvester.about', id=source['name'])
        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        response = app.get(url, extra_environ=env)

        assert source['name'] in response.body

    def test_job_page_rendered(self, app):

        job = harvest_factories.HarvestJob()

        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        url = url_for('harvester.job_list', source=job['source_id'])

        response = app.get(url, extra_environ=env)

        assert job['id'] in response.body

    def test_job_show_last_page_rendered(self, app):

        job = harvest_factories.HarvestJob()

        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        url = url_for('harvester.job_show_last', source=job['source_id'])

        response = app.get(url, extra_environ=env)

        assert job['id'] in response.body

    def test_job_show_page_rendered(self, app):

        job = harvest_factories.HarvestJob()

        url = url_for(
            'harvester.job_show', source=job['source_id'], id=job['id'])
        sysadmin = factories.Sysadmin()
        env = {"REMOTE_USER": sysadmin['name'].encode('ascii')}

        response = app.get(url, extra_environ=env)

        assert job['id'] in response.body

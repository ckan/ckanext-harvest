from ckan.tests import *

from ckanext.harvest.model import HarvestSource

import ckan.model as model
from ckan.tests.pylons_controller import PylonsTestCase
import ckanext.dgu.forms.harvest_source as form

from nose.plugins.skip import SkipTest;
raise SkipTest('These tests should be moved to ckanext-harvest.')

class TestHarvestSource(PylonsTestCase):

    @classmethod
    def setup_class(cls):
        super(TestHarvestSource, cls).setup_class()

    def setup(self):
        model.repo.init_db()

    def teardown(self):
        model.repo.rebuild_db()

    def test_form_raw(self):
        fs = form.get_harvest_source_fieldset()
        text = fs.render()
        assert 'url' in text
        assert 'type' in text
        assert 'description' in text

    def test_form_bound_to_existing_object(self):
        source = HarvestSource(url=u'http://localhost/', description=u'My source', type=u'Gemini')
        model.Session.add(source)
        model.Session.commit()
        model.Session.remove()
        fs = form.get_harvest_source_fieldset()
        fs = fs.bind(source)
        text = fs.render()
        assert 'url' in text
        assert 'http://localhost/' in text
        assert 'description' in text
        assert 'My source' in text

    def test_form_bound_to_new_object(self):
        source = HarvestSource(url=u'http://localhost/', description=u'My source', type=u'Gemini')
        fs = form.get_harvest_source_fieldset()
        fs = fs.bind(source)
        text = fs.render()
        assert 'url' in text
        assert 'http://localhost/' in text
        assert 'description' in text
        assert 'My source' in text

    def test_form_validate_new_object_and_sync(self):
        assert not HarvestSource.get(u'http://localhost/', None, 'url')
        fs = form.get_harvest_source_fieldset()
        register = HarvestSource
        data = {
            'HarvestSource--url': u'http://localhost/', 
            'HarvestSource--type': u'Gemini',
            'HarvestSource--description': u'My source'
        }
        fs = fs.bind(register, data=data, session=model.Session)
        # Test bound_fields.validate().
        fs.validate()
        assert not fs.errors
        # Test bound_fields.sync().
        fs.sync()
        model.Session.commit()
        source = HarvestSource.get(u'http://localhost/', None, 'url')
        assert source.id

    def test_form_invalidate_new_object_null(self):
        fs = form.get_harvest_source_fieldset()
        register = HarvestSource
        data = {
            'HarvestSource--url': u'', 
            'HarvestSource--type': u'Gemini',
            'HarvestSource--description': u'My source'
        }
        fs = fs.bind(register, data=data)
        # Test bound_fields.validate().
        fs.validate()
        assert fs.errors

    def test_form_invalidate_new_object_not_http(self):
        fs = form.get_harvest_source_fieldset()
        register = HarvestSource
        data = {
            'HarvestSource--url': u'htp:', 
            'HarvestSource--type': u'Gemini',
            'HarvestSource--description': u'My source'
        }
        fs = fs.bind(register, data=data)
        # Test bound_fields.validate().
        fs.validate()
        assert fs.errors

    def test_form_invalidate_new_object_no_type(self):
        fs = form.get_harvest_source_fieldset()
        register = HarvestSource
        data = {
            'HarvestSource--url': u'htp:', 
            'HarvestSource--type': u'',
            'HarvestSource--description': u'My source'
        }
        fs = fs.bind(register, data=data)
        # Test bound_fields.validate().
        fs.validate()
        assert fs.errors


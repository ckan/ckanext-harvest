import re

from nose.tools import assert_equal
from ckanext.harvest import model as harvest_model
from ckanext.harvest.harvesters.base import HarvesterBase
try:
    from ckan.tests import helpers
    from ckan.tests import factories
except ImportError:
    from ckan.new_tests import helpers
    from ckan.new_tests import factories


_ensure_name_is_unique = HarvesterBase._ensure_name_is_unique


class TestGenNewName(object):
    @classmethod
    def setup_class(cls):
        helpers.reset_db()
        harvest_model.setup()

    def test_basic(self):
        assert_equal(HarvesterBase._gen_new_name('Trees'), 'trees')

    def test_munge(self):
        assert_equal(
            HarvesterBase._gen_new_name('Trees and branches - survey.'),
            'trees-and-branches-survey')


class TestEnsureNameIsUnique(object):
    def setup(self):
        helpers.reset_db()
        harvest_model.setup()

    def test_no_existing_datasets(self):
        factories.Dataset(name='unrelated')
        assert_equal(_ensure_name_is_unique('trees'), 'trees')

    def test_existing_dataset(self):
        factories.Dataset(name='trees')
        assert_equal(_ensure_name_is_unique('trees'), 'trees1')

    def test_two_existing_datasets(self):
        factories.Dataset(name='trees')
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('trees'), 'trees2')

    def test_no_existing_datasets_and_long_name(self):
        assert_equal(_ensure_name_is_unique('x'*101), 'x'*100)

    def test_existing_dataset_and_long_name(self):
        # because PACKAGE_NAME_MAX_LENGTH = 100
        factories.Dataset(name='x'*100)
        assert_equal(_ensure_name_is_unique('x'*101), 'x'*99 + '1')

    def test_update_dataset_with_new_name(self):
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('tree', existing_name='trees1'),
                     'tree')

    def test_update_dataset_but_with_same_name(self):
        # this can happen if you remove a trailing space from the title - the
        # harvester sees the title changed and thinks it should have a new
        # name, but clearly it can reuse its existing one
        factories.Dataset(name='trees')
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('trees', existing_name='trees'),
                     'trees')

    def test_update_dataset_to_available_shorter_name(self):
        # this can be handy when if reharvesting, you got duplicates and
        # managed to purge one set and through a minor title change you can now
        # lose the appended number. users don't like unnecessary numbers.
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('trees', existing_name='trees1'),
                     'trees')

    def test_update_dataset_but_doesnt_change_to_other_number(self):
        # there's no point changing one number for another though
        factories.Dataset(name='trees')
        factories.Dataset(name='trees2')
        assert_equal(_ensure_name_is_unique('trees', existing_name='trees2'),
                     'trees2')

    def test_update_dataset_with_new_name_with_numbers(self):
        factories.Dataset(name='trees')
        factories.Dataset(name='trees2')
        factories.Dataset(name='frogs')
        assert_equal(_ensure_name_is_unique('frogs', existing_name='trees2'),
                     'frogs1')

    def test_existing_dataset_appending_hex(self):
        factories.Dataset(name='trees')
        name = _ensure_name_is_unique('trees', append_type='random-hex')
        # e.g. 'trees0b53f'
        assert re.match('trees[\da-f]{5}', name)

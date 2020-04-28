import re

import pytest
from mock import patch

from ckanext.harvest.harvesters.base import HarvesterBase, munge_tag
from ckantoolkit.tests import factories

_ensure_name_is_unique = HarvesterBase._ensure_name_is_unique


@pytest.mark.usefixtures('clean_db', 'clean_index', 'harvest_setup')
class TestGenNewName(object):

    def test_basic(self):
        assert HarvesterBase._gen_new_name('Trees') == 'trees'

    def test_munge(self):
        assert HarvesterBase._gen_new_name('Trees and branches - survey.') == 'trees-and-branches-survey'

    @patch.dict('ckanext.harvest.harvesters.base.config',
                {'ckanext.harvest.some_other_config': 'value'})
    def test_without_config(self):
        '''Tests if the number suffix is used when no config is set.'''
        factories.Dataset(name='trees')
        assert HarvesterBase._gen_new_name('Trees') == 'trees1'

    @patch.dict('ckanext.harvest.harvesters.base.config',
                {'ckanext.harvest.default_dataset_name_append': 'number-sequence'})
    def test_number_config(self):
        factories.Dataset(name='trees')
        assert HarvesterBase._gen_new_name('Trees') == 'trees1'

    @patch.dict('ckanext.harvest.harvesters.base.config',
                {'ckanext.harvest.default_dataset_name_append': 'random-hex'})
    def test_random_config(self):
        factories.Dataset(name='trees')
        new_name = HarvesterBase._gen_new_name('Trees')

        assert re.match(r'trees[\da-f]{5}', new_name)

    @patch.dict('ckanext.harvest.harvesters.base.config',
                {'ckanext.harvest.default_dataset_name_append': 'random-hex'})
    def test_config_override(self):
        '''Tests if a parameter has precedence over a config value.'''
        factories.Dataset(name='trees')
        assert HarvesterBase._gen_new_name('Trees', append_type='number-sequence') == 'trees1'


@pytest.mark.usefixtures('clean_db', 'clean_index', 'harvest_setup')
class TestEnsureNameIsUnique(object):

    def test_no_existing_datasets(self):
        factories.Dataset(name='unrelated')
        assert _ensure_name_is_unique('trees') == 'trees'

    def test_existing_dataset(self):
        factories.Dataset(name='trees')
        assert _ensure_name_is_unique('trees') == 'trees1'

    def test_two_existing_datasets(self):
        factories.Dataset(name='trees')
        factories.Dataset(name='trees1')
        assert _ensure_name_is_unique('trees') == 'trees2'

    def test_no_existing_datasets_and_long_name(self):
        assert _ensure_name_is_unique('x' * 101) == 'x' * 100

    def test_existing_dataset_and_long_name(self):
        # because PACKAGE_NAME_MAX_LENGTH = 100
        factories.Dataset(name='x' * 100)
        assert _ensure_name_is_unique('x' * 101) == 'x' * 99 + '1'

    def test_update_dataset_with_new_name(self):
        factories.Dataset(name='trees1')
        assert _ensure_name_is_unique('tree', existing_name='trees1') == 'tree'

    def test_update_dataset_but_with_same_name(self):
        # this can happen if you remove a trailing space from the title - the
        # harvester sees the title changed and thinks it should have a new
        # name, but clearly it can reuse its existing one
        factories.Dataset(name='trees')
        factories.Dataset(name='trees1')
        assert _ensure_name_is_unique('trees', existing_name='trees') == 'trees'

    def test_update_dataset_to_available_shorter_name(self):
        # this can be handy when if reharvesting, you got duplicates and
        # managed to purge one set and through a minor title change you can now
        # lose the appended number. users don't like unnecessary numbers.
        factories.Dataset(name='trees1')
        assert _ensure_name_is_unique('trees', existing_name='trees1') == 'trees'

    def test_update_dataset_but_doesnt_change_to_other_number(self):
        # there's no point changing one number for another though
        factories.Dataset(name='trees')
        factories.Dataset(name='trees2')
        assert _ensure_name_is_unique('trees', existing_name='trees2') == 'trees2'

    def test_update_dataset_with_new_name_with_numbers(self):
        factories.Dataset(name='trees')
        factories.Dataset(name='trees2')
        factories.Dataset(name='frogs')
        assert _ensure_name_is_unique('frogs', existing_name='trees2') == 'frogs1'

    def test_existing_dataset_appending_hex(self):
        factories.Dataset(name='trees')
        name = _ensure_name_is_unique('trees', append_type='random-hex')
        # e.g. 'trees0b53f'
        assert re.match(r'trees[\da-f]{5}', name)


# taken from ckan/tests/lib/test_munge.py
class TestMungeTag:

    # (original, expected)
    munge_list = [
        ('unchanged', 'unchanged'),
        # ('s', 's_'),  # too short
        ('some spaces  here', 'some-spaces--here'),
        ('random:other%characters&_.here', 'randomothercharactershere'),
        ('river-water-dashes', 'river-water-dashes'),
    ]

    def test_munge_tag(self):
        '''Munge a list of tags gives expected results.'''
        for org, exp in self.munge_list:
            munge = munge_tag(org)
            assert munge == exp

    def test_munge_tag_multiple_pass(self):
        '''Munge a list of tags muliple times gives expected results.'''
        for org, exp in self.munge_list:
            first_munge = munge_tag(org)
            assert first_munge == exp
            second_munge = munge_tag(first_munge)
            assert second_munge == exp

    def test_clean_tags_package_show(self):
        instance = HarvesterBase()
        tags_as_dict = [{u'vocabulary_id': None,
                         u'state': u'active',
                         u'display_name': name,
                         u'id': u'073080c8-fef2-4743-9c9e-6216019f8b3d',
                         u'name': name} for name, exp in self.munge_list]

        clean_tags = HarvesterBase._clean_tags(instance, tags_as_dict)

        idx = 0
        for _, exp in self.munge_list:
            tag = clean_tags[idx]
            assert tag['name'] == exp
            idx += 1

    def test_clean_tags_rest(self):
        instance = HarvesterBase()
        tags_as_str = [name for name, exp in self.munge_list]

        clean_tags = HarvesterBase._clean_tags(instance, tags_as_str)

        assert len(clean_tags) == len(tags_as_str)

        for _, exp in self.munge_list:
            assert exp in clean_tags

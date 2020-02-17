from __future__ import absolute_import
import six
import requests
from requests.exceptions import HTTPError, RequestException

import datetime
from urllib3.contrib import pyopenssl

from six.moves.urllib.parse import urlencode
from ckan import model
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.plugins import toolkit

from ckanext.harvest.model import HarvestObject
from .base import HarvesterBase

import logging
log = logging.getLogger(__name__)


class CKANHarvester(HarvesterBase):
    '''
    A Harvester for CKAN instances
    '''
    config = None

    api_version = 2
    action_api_version = 3

    def _get_action_api_offset(self):
        return '/api/%d/action' % self.action_api_version

    def _get_search_api_offset(self):
        return '%s/package_search' % self._get_action_api_offset()

    def _get_content(self, url):

        headers = {}
        api_key = self.config.get('api_key')
        if api_key:
            headers['Authorization'] = api_key

        pyopenssl.inject_into_urllib3()

        try:
            http_request = requests.get(url, headers=headers)
        except HTTPError as e:
            raise ContentFetchError('HTTP error: %s %s' % (e.response.status_code, e.request.url))
        except RequestException as e:
            raise ContentFetchError('Request error: %s' % e)
        except Exception as e:
            raise ContentFetchError('HTTP general exception: %s' % e)
        return http_request.text

    def _get_group(self, base_url, group):
        url = base_url + self._get_action_api_offset() + '/group_show?id=' + \
            group['id']
        try:
            content = self._get_content(url)
            data = json.loads(content)
            if self.action_api_version == 3:
                return data.pop('result')
            return data
        except (ContentFetchError, ValueError):
            log.debug('Could not fetch/decode remote group')
            raise RemoteResourceError('Could not fetch/decode remote group')

    def _get_organization(self, base_url, org_name):
        url = base_url + self._get_action_api_offset() + \
            '/organization_show?id=' + org_name
        try:
            content = self._get_content(url)
            content_dict = json.loads(content)
            return content_dict['result']
        except (ContentFetchError, ValueError, KeyError):
            log.debug('Could not fetch/decode remote group')
            raise RemoteResourceError(
                'Could not fetch/decode remote organization')

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': 'ckan',
            'title': 'CKAN',
            'description': 'Harvests remote CKAN instances',
            'form_config_interface': 'Text'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'], list):
                    raise ValueError('default_tags must be a list')
                if config_obj['default_tags'] and \
                        not isinstance(config_obj['default_tags'][0], dict):
                    raise ValueError('default_tags must be a list of '
                                     'dictionaries')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'], list):
                    raise ValueError('default_groups must be a *list* of group'
                                     ' names/ids')
                if config_obj['default_groups'] and \
                        not isinstance(config_obj['default_groups'][0],
                                       six.string_types):
                    raise ValueError('default_groups must be a list of group '
                                     'names/ids (i.e. strings)')

                # Check if default groups exist
                context = {'model': model, 'user': toolkit.c.user}
                config_obj['default_group_dicts'] = []
                for group_name_or_id in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(
                            context, {'id': group_name_or_id})
                        # save the dict to the config object, as we'll need it
                        # in the import_stage of every dataset
                        config_obj['default_group_dicts'].append(group)
                    except NotFound as e:
                        raise ValueError('Default group not found')
                config = json.dumps(config_obj)

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'], dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'organizations_filter_include' in config_obj \
                    and 'organizations_filter_exclude' in config_obj:
                raise ValueError('Harvest configuration cannot contain both '
                                 'organizations_filter_include and organizations_filter_exclude')

            if 'groups_filter_include' in config_obj \
                    and 'groups_filter_exclude' in config_obj:
                raise ValueError('Harvest configuration cannot contain both '
                                 'groups_filter_include and groups_filter_exclude')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model': model, 'user': toolkit.c.user}
                try:
                    get_action('user_show')(
                        context, {'id': config_obj.get('user')})
                except NotFound:
                    raise ValueError('User not found')

            for key in ('read_only', 'force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key], bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError as e:
            raise e

        return config

    def modify_package_dict(self, package_dict, harvest_object):
        '''
            Allows custom harvesters to modify the package dict before
            creating or updating the actual package.
        '''
        return package_dict

    def gather_stage(self, harvest_job):
        log.debug('In CKANHarvester gather_stage (%s)',
                  harvest_job.source.url)
        toolkit.requires_ckan_version(min_version='2.0')
        get_all_packages = True

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_ckan_base_url = harvest_job.source.url.rstrip('/')

        # Filter in/out datasets from particular organizations
        fq_terms = []
        org_filter_include = self.config.get('organizations_filter_include', [])
        org_filter_exclude = self.config.get('organizations_filter_exclude', [])
        if org_filter_include:
            fq_terms.append(' OR '.join(
                'organization:%s' % org_name for org_name in org_filter_include))
        elif org_filter_exclude:
            fq_terms.extend(
                '-organization:%s' % org_name for org_name in org_filter_exclude)

        groups_filter_include = self.config.get('groups_filter_include', [])
        groups_filter_exclude = self.config.get('groups_filter_exclude', [])
        if groups_filter_include:
            fq_terms.append(' OR '.join(
                'groups:%s' % group_name for group_name in groups_filter_include))
        elif groups_filter_exclude:
            fq_terms.extend(
                '-groups:%s' % group_name for group_name in groups_filter_exclude)

        # Ideally we can request from the remote CKAN only those datasets
        # modified since the last completely successful harvest.
        last_error_free_job = self.last_error_free_job(harvest_job)
        log.debug('Last error-free job: %r', last_error_free_job)
        if (last_error_free_job and
                not self.config.get('force_all', False)):
            get_all_packages = False

            # Request only the datasets modified since
            last_time = last_error_free_job.gather_started
            # Note: SOLR works in UTC, and gather_started is also UTC, so
            # this should work as long as local and remote clocks are
            # relatively accurate. Going back a little earlier, just in case.
            get_changes_since = \
                (last_time - datetime.timedelta(hours=1)).isoformat()
            log.info('Searching for datasets modified since: %s UTC',
                     get_changes_since)

            fq_since_last_time = 'metadata_modified:[{since}Z TO *]' \
                .format(since=get_changes_since)

            try:
                pkg_dicts = self._search_for_datasets(
                    remote_ckan_base_url,
                    fq_terms + [fq_since_last_time])
            except SearchError as e:
                log.info('Searching for datasets changed since last time '
                         'gave an error: %s', e)
                get_all_packages = True

            if not get_all_packages and not pkg_dicts:
                log.info('No datasets have been updated on the remote '
                         'CKAN instance since the last harvest job %s',
                         last_time)
                return []

        # Fall-back option - request all the datasets from the remote CKAN
        if get_all_packages:
            # Request all remote packages
            try:
                pkg_dicts = self._search_for_datasets(remote_ckan_base_url,
                                                      fq_terms)
            except SearchError as e:
                log.info('Searching for all datasets gave an error: %s', e)
                self._save_gather_error(
                    'Unable to search remote CKAN for datasets:%s url:%s'
                    'terms:%s' % (e, remote_ckan_base_url, fq_terms),
                    harvest_job)
                return None
        if not pkg_dicts:
            self._save_gather_error(
                'No datasets found at CKAN: %s' % remote_ckan_base_url,
                harvest_job)
            return []

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []
            for pkg_dict in pkg_dicts:
                if pkg_dict['id'] in package_ids:
                    log.info('Discarding duplicate dataset %s - probably due '
                             'to datasets being changed at the same time as '
                             'when the harvester was paging through',
                             pkg_dict['id'])
                    continue
                package_ids.add(pkg_dict['id'])

                log.debug('Creating HarvestObject for %s %s',
                          pkg_dict['name'], pkg_dict['id'])
                obj = HarvestObject(guid=pkg_dict['id'],
                                    job=harvest_job,
                                    content=json.dumps(pkg_dict))
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception as e:
            self._save_gather_error('%r' % e.message, harvest_job)

    def _search_for_datasets(self, remote_ckan_base_url, fq_terms=None):
        '''Does a dataset search on a remote CKAN and returns the results.

        Deals with paging to return all the results, not just the first page.
        '''
        base_search_url = remote_ckan_base_url + self._get_search_api_offset()
        params = {'rows': '100', 'start': '0'}
        # There is the worry that datasets will be changed whilst we are paging
        # through them.
        # * In SOLR 4.7 there is a cursor, but not using that yet
        #   because few CKANs are running that version yet.
        # * However we sort, then new names added or removed before the current
        #   page would cause existing names on the next page to be missed or
        #   double counted.
        # * Another approach might be to sort by metadata_modified and always
        #   ask for changes since (and including) the date of the last item of
        #   the day before. However if the entire page is of the exact same
        #   time, then you end up in an infinite loop asking for the same page.
        # * We choose a balanced approach of sorting by ID, which means
        #   datasets are only missed if some are removed, which is far less
        #   likely than any being added. If some are missed then it is assumed
        #   they will harvested the next time anyway. When datasets are added,
        #   we are at risk of seeing datasets twice in the paging, so we detect
        #   and remove any duplicates.
        params['sort'] = 'id asc'
        if fq_terms:
            params['fq'] = ' '.join(fq_terms)

        pkg_dicts = []
        pkg_ids = set()
        previous_content = None
        while True:
            url = base_search_url + '?' + urlencode(params)
            log.debug('Searching for CKAN datasets: %s', url)
            try:
                content = self._get_content(url)
            except ContentFetchError as e:
                raise SearchError(
                    'Error sending request to search remote '
                    'CKAN instance %s using URL %r. Error: %s' %
                    (remote_ckan_base_url, url, e))

            if previous_content and content == previous_content:
                raise SearchError('The paging doesn\'t seem to work. URL: %s' %
                                  url)
            try:
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError('Response from remote CKAN was not JSON: %r'
                                  % content)
            try:
                pkg_dicts_page = response_dict.get('result', {}).get('results',
                                                                     [])
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'result/results: %r' % response_dict)

            # Weed out any datasets found on previous pages (should datasets be
            # changing while we page)
            ids_in_page = set(p['id'] for p in pkg_dicts_page)
            duplicate_ids = ids_in_page & pkg_ids
            if duplicate_ids:
                pkg_dicts_page = [p for p in pkg_dicts_page
                                  if p['id'] not in duplicate_ids]
            pkg_ids |= ids_in_page

            pkg_dicts.extend(pkg_dicts_page)

            if len(pkg_dicts_page) == 0:
                break

            params['start'] = str(int(params['start']) + int(params['rows']))

        return pkg_dicts

    def fetch_stage(self, harvest_object):
        # Nothing to do here - we got the package dict in the search in the
        # gather stage
        return True

    def import_stage(self, harvest_object):
        log.debug('In CKANHarvester import_stage')

        base_context = {'model': model, 'session': model.Session,
                        'user': self._get_user_name()}
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)

            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            # Set default tags if needed
            default_tags = self.config.get('default_tags', [])
            if default_tags:
                if 'tags' not in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', None)
            if remote_groups not in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if 'groups' not in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []

                for group_ in package_dict['groups']:
                    try:
                        try:
                            if 'id' in group_:
                                data_dict = {'id': group_['id']}
                                group = get_action('group_show')(base_context.copy(), data_dict)
                            else:
                                raise NotFound

                        except NotFound as e:
                            if 'name' in group_:
                                data_dict = {'id': group_['name']}
                                group = get_action('group_show')(base_context.copy(), data_dict)
                            else:
                                raise NotFound
                        # Found local group
                        validated_groups.append({'id': group['id'], 'name': group['name']})

                    except NotFound as e:
                        log.info('Group %s is not available', group_)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_)
                            except RemoteResourceError:
                                log.error('Could not get remote group %s', group_)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)

                            get_action('group_create')(base_context.copy(), group)
                            log.info('Group %s has been newly created', group_)
                            validated_groups.append({'id': group['id'], 'name': group['name']})

                package_dict['groups'] = validated_groups

            # Local harvest source organization
            source_dataset = get_action('package_show')(base_context.copy(), {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', None)

            if remote_orgs not in ('only_local', 'create'):
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                if 'owner_org' not in package_dict:
                    package_dict['owner_org'] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict['owner_org']

                if remote_org:
                    try:
                        data_dict = {'id': remote_org}
                        org = get_action('organization_show')(base_context.copy(), data_dict)
                        validated_org = org['id']
                    except NotFound as e:
                        log.info('Organization %s is not available', remote_org)
                        if remote_orgs == 'create':
                            try:
                                try:
                                    org = self._get_organization(harvest_object.source.url, remote_org)
                                except RemoteResourceError:
                                    # fallback if remote CKAN exposes organizations as groups
                                    # this especially targets older versions of CKAN
                                    org = self._get_group(harvest_object.source.url, remote_org)

                                for key in ['packages', 'created', 'users', 'groups', 'tags',
                                            'extras', 'display_name', 'type']:
                                    org.pop(key, None)
                                get_action('organization_create')(base_context.copy(), org)
                                log.info('Organization %s has been newly created', remote_org)
                                validated_org = org['id']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                package_dict['owner_org'] = validated_org or local_org

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                if 'groups' not in package_dict:
                    package_dict['groups'] = []
                existing_group_ids = [g['id'] for g in package_dict['groups']]
                package_dict['groups'].extend(
                    [g for g in self.config['default_group_dicts']
                     if g['id'] not in existing_group_ids])

            # Set default extras if needed
            default_extras = self.config.get('default_extras', {})

            def get_extra(key, package_dict):
                for extra in package_dict.get('extras', []):
                    if extra['key'] == key:
                        return extra
            if default_extras:
                override_extras = self.config.get('override_extras', False)
                if 'extras' not in package_dict:
                    package_dict['extras'] = []
                for key, value in default_extras.items():
                    existing_extra = get_extra(key, package_dict)
                    if existing_extra and not override_extras:
                        continue  # no need for the default
                    if existing_extra:
                        package_dict['extras'].remove(existing_extra)
                    # Look for replacement strings
                    if isinstance(value, six.string_types):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip('/'),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict['id'])

                    package_dict['extras'].append({'key': key, 'value': value})

            for resource in package_dict.get('resources', []):
                # Clear remote url_type for resources (eg datastore, upload) as
                # we are only creating normal resources with links to the
                # remote ones
                resource.pop('url_type', None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop('revision_id', None)

            package_dict = self.modify_package_dict(package_dict, harvest_object)

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form='package_show')

            return result
        except ValidationError as e:
            self._save_object_error('Invalid package with GUID %s: %r' %
                                    (harvest_object.guid, e.error_dict),
                                    harvest_object, 'Import')
        except Exception as e:
            self._save_object_error('%s' % e, harvest_object, 'Import')


class ContentFetchError(Exception):
    pass


class ContentNotFoundError(ContentFetchError):
    pass


class RemoteResourceError(Exception):
    pass


class SearchError(Exception):
    pass

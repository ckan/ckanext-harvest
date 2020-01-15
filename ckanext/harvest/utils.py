# -*- coding: utf-8 -*-

from __future__ import print_function
import ckan.plugins.toolkit as tk
import ckan.model as model
from six import StringIO
DATASET_TYPE_NAME = 'harvest'


def _admin_user():
    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    return tk.get_action('get_site_user')(context, {})


def _print_harvest_source(source, output):
    print('Source id: {0}'.format(source.get('id')), file=output)
    if 'name' in source:
        # 'name' is only there if the source comes from the Package
        print('\tname: {}'.format(source['name']), file=output)

    data_dict = {
        'url': source.get('url'),
        # 'type' if source comes from HarvestSource, 'source_type' if
        # it comes from the Package
        'type': source.get('source_type') or source.get('type'),
        'active': source.get('active',
                             source.get('state') == 'active'),
        'frequency': source.get('frequency'),
        'jobs': source.get('status').get('job_count')
    }
    print(('\turl: {url}'
           '\ttype: {type}'
           '\tactive: {active}'
           '\tfrequency: {frequency}'
           '\tjobs: {jobs}'
           '').format(**data_dict),
          file=output)


def _there_are(what, sequence, condition=''):
    is_singular = len(sequence) == 1
    return ('There {0} {1} {2}{3}{4}'.format(
        is_singular and 'is' or 'are',
        len(sequence),
        condition and ('{0} '.format(condition.lower())) or '',
        what,
        not is_singular and 's' or '',
    ))


def initdb():
    from ckanext.harvest.model import setup as db_setup
    db_setup()


def create_harvest_source(name,
                          url,
                          type,
                          title=None,
                          active=True,
                          owner_org=None,
                          frequency='MANUAL',
                          config=None):
    output = StringIO()

    data_dict = {
        'name': name,
        'url': url,
        'source_type': type,
        'title': title,
        'active': active,
        'owner_org': owner_org,
        'frequency': frequency,
        'config': config,
    }

    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name'],
        'ignore_auth': True,
    }
    source = tk.get_action('harvest_source_create')(context, data_dict)

    print('Created new harvest source:', file=output)
    _print_harvest_source(source, output)

    sources = tk.get_action('harvest_source_list')(context, {})
    print(_there_are('harvest source', sources), file=output)

    # Create a harvest job for the new source if not regular job.
    if not data_dict['frequency']:
        tk.get_action('harvest_job_create')(context, {
            'source_id': source['id'],
            'run': True
        })
        print('A new Harvest Job for this source has also been created',
              file=output)

    return output.getvalue()


def show_harvest_source(source_id_or_name):
    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    source = tk.get_action('harvest_source_show')(context, {
        'id': source_id_or_name
    })
    output = StringIO()
    _print_harvest_source(source, output)
    return output.getvalue()

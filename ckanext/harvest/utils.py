# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import ckan.plugins.toolkit as tk
import ckan.model as model
from six import StringIO
import six

DATASET_TYPE_NAME = 'harvest'


def _admin_user():
    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    return tk.get_action('get_site_user')(context, {})


def _print_harvest_sources(sources, output):
    for source in sources:
        _print_harvest_source(source, output)


def _print_harvest_jobs(jobs, output):
    for job in jobs:
        _print_harvest_job(job, output)


def _print_harvest_job(job, output):
    print(('\tJob id: {}\n'
           '\t\tstatus: {}\n'
           '\t\tsource: {}\n'
           '\t\tobjects: {}').format(job.get('id'), job.get('status'),
                                     job.get('source_id'),
                                     len(job.get('objects', []))),
          file=output)

    print('\t\tgather_errors: {0}'.format(len(job.get('gather_errors', []))),
          file=output)
    for error in job.get('gather_errors', []):
        print('\t\t\t{0}'.format(error['message']), file=output)


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
    print(('\turl: {url}\n'
           '\ttype: {type}\n'
           '\tactive: {active}\n'
           '\tfrequency: {frequency}\n'
           '\tjobs: {jobs}\n'
           '\n').format(**data_dict),
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


def remove_harvest_source(source_id_or_name):
    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    source = tk.get_action('harvest_source_show')(context, {
        'id': source_id_or_name
    })
    tk.get_action('harvest_source_delete')(context, {'id': source['id']})


def clear_harvest_source(source_id_or_name):
    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    source = tk.get_action('harvest_source_show')(context, {
        'id': source_id_or_name
    })
    tk.get_action('harvest_source_clear')(context, {'id': source['id']})


def clear_harvest_source_history(source_id):

    context = {
        'model': model,
        'user': _admin_user()['name'],
        'session': model.Session
    }
    if source_id is not None:
        tk.get_action('harvest_source_job_history_clear')(context, {
            'id': source_id
        })
        return ('Cleared job history of harvest source: {0}'.format(source_id))
    else:
        # Purge queues, because we clean all harvest jobs and
        # objects in the database.
        purge_queues()
        cleared_sources_dicts = tk.get_action(
            'harvest_sources_job_history_clear')(context, {})
        return ('Cleared job history for all harvest sources: {0} source(s)'.
                format(len(cleared_sources_dicts)))


def purge_queues():
    from ckanext.harvest.queue import purge_queues as purge
    purge()


def list_sources(all):
    if all:
        data_dict = {}
        what = 'harvest source'
    else:
        data_dict = {'only_active': True}
        what = 'active harvest source'

    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    sources = tk.get_action('harvest_source_list')(context, data_dict)
    output = StringIO()
    _print_harvest_sources(sources, output)
    print(_there_are(what, sources), file=output)
    return output.getvalue()


def create_job(source_id_or_name):
    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    source = tk.get_action('harvest_source_show')(context, {
        'id': source_id_or_name
    })

    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    job = tk.get_action('harvest_job_create')(context, {
        'source_id': source['id'],
        'run': True
    })

    output = StringIO()
    _print_harvest_job(job, output)
    jobs = tk.get_action('harvest_job_list')(context, {'status': u'New'})
    print(_there_are('harvest job', jobs, condition=u'New'), file=output)
    return output.getvalue()


def list_jobs():
    context = {
        'model': model,
        'user': _admin_user()['name'],
        'session': model.Session
    }
    jobs = tk.get_action('harvest_job_list')(context, {})

    output = StringIO()
    _print_harvest_jobs(jobs, output)
    print(_there_are(what='harvest job', sequence=jobs), file=output)
    return output.getvalue()


def abort_job(job_or_source_id_or_name):
    context = {
        'model': model,
        'user': _admin_user()['name'],
        'session': model.Session
    }
    job = tk.get_action('harvest_job_abort')(context, {
        'id': job_or_source_id_or_name
    })
    return ('Job status: {0}'.format(job['status']))


def gather_consumer():
    import logging
    from ckanext.harvest.queue import (get_gather_consumer, gather_callback,
                                       get_gather_queue_name)
    logging.getLogger('amqplib').setLevel(logging.INFO)
    consumer = get_gather_consumer()
    for method, header, body in consumer.consume(
            queue=get_gather_queue_name()):
        gather_callback(consumer, method, header, body)


def fetch_consumer():
    import logging
    logging.getLogger('amqplib').setLevel(logging.INFO)
    from ckanext.harvest.queue import (get_fetch_consumer, fetch_callback,
                                       get_fetch_queue_name)
    consumer = get_fetch_consumer()
    for method, header, body in consumer.consume(queue=get_fetch_queue_name()):
        fetch_callback(consumer, method, header, body)


def run_harvester():
    context = {
        'model': model,
        'user': _admin_user()['name'],
        'session': model.Session
    }
    tk.get_action('harvest_jobs_run')(context, {})


def run_test_harvester(source_id_or_name):
    from ckanext.harvest import queue
    from ckanext.harvest.tests import lib
    from ckanext.harvest.logic import HarvestJobExists
    from ckanext.harvest.model import HarvestJob

    context = {
        'model': model,
        'session': model.Session,
        'user': _admin_user()['name']
    }
    source = tk.get_action('harvest_source_show')(context, {
        'id': source_id_or_name
    })

    # Determine the job
    try:
        job_dict = tk.get_action('harvest_job_create')(
            context, {
                'source_id': source['id']
            })
    except HarvestJobExists:
        running_jobs = tk.get_action('harvest_job_list')(
            context, {
                'source_id': source['id'],
                'status': 'Running'
            })
        if running_jobs:
            print('\nSource "{0}" apparently has a "Running" job:\n{1}'.format(
                source.get('name') or source['id'], running_jobs))

            if six.PY2:
                resp = raw_input('Abort it? (y/n)')
            else:
                resp = input('Abort it? (y/n)')
            if not resp.lower().startswith('y'):
                sys.exit(1)
            job_dict = tk.get_action('harvest_job_abort')(
                context, {
                    'source_id': source['id']
                })
        else:
            print('Reusing existing harvest job')
            jobs = tk.get_action('harvest_job_list')(context, {
                'source_id': source['id'],
                'status': 'New'
            })
            assert len(jobs) == 1, \
                'Multiple "New" jobs for this source! {0}'.format(jobs)
            job_dict = jobs[0]
    job_obj = HarvestJob.get(job_dict['id'])

    harvester = queue.get_harvester(source['source_type'])
    assert harvester, \
        'No harvester found for type: {0}'.format(source['source_type'])
    lib.run_harvest_job(job_obj, harvester)

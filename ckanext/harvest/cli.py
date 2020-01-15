# -*- coding: utf-8 -*-

from __future__ import print_function
import click
import six

import sys
from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError
import ckantoolkit as tk

import ckanext.harvest.utils as utils
from ckanext.harvest.logic import HarvestJobExists

def get_commands():
    return [harvester]

@click.group()
def harvester():
    """Harvests remotely mastered metadata.
    """
    pass

@harvester.command()
def initdb():
    """Creates the necessary tables in the database.
    """
    utils.initdb()
    click.secho(u'DB tables created', fg=u'green')

@harvester.group()
def source():
    """Manage harvest sources
    """
    pass

@source.command()
@click.argument(u'name')
@click.argument(u'url')
@click.argument(u'type')
@click.argument(u'title', required=False)
@click.argument(u'active', type=tk.asbool, default=True)
@click.argument(u'owner_org', required=False)
@click.argument(u'frequency', default=u'MANUAL')
@click.argument(u'config', required=False)
def create(name, url, type, title, active, owner_org, frequency, config):
    """Create new harvest source.
    """
    try:
        result = utils.create_harvest_source(
            name, url, type, title, active, owner_org, frequency, config
        )
    except tk.ValidationError as e:
        tk.error_shout(u'Validation error:')
        for field, err in e.error_summary.items():
            tk.error_shout('\t{}: {}'.format(field, err))
        raise click.Abort()
    click.echo(result)


@source.command()
@click.argument(u'id', metavar=u'SOURCE_ID_OR_NAME')
@click.pass_context
def show(ctx, id):
    """Shows a harvest source.
    """
    flask_app = ctx.meta['flask_app']

    try:
        with flask_app.test_request_context():
            result = utils.show_harvest_source(id)
    except tk.ObjectNotFound as e:
        tk.error_shout(u'Source <{}> not found.'.format(id))
        raise click.Abort()
    click.echo(result)


@source.command()
@click.argument(u'id', metavar=u'SOURCE_ID_OR_NAME')
@click.pass_context
def remove(ctx, id):
    """Remove (deactivate) a harvester source, whilst leaving any related
    datasets, jobs and objects.

    """
    flask_app = ctx.meta['flask_app']

    with flask_app.test_request_context():
        utils.remove_harvest_source(id)
    click.secho('Removed harvest source: {0}'.format(id), fg='green')

@source.command()
@click.argument(u'id', metavar=u'SOURCE_ID_OR_NAME')
@click.pass_context
def clear(ctx, id):
    """Clears all datasets, jobs and objects related to a harvest source,
    but keeps the source itself.

    """
    flask_app = ctx.meta['flask_app']

    with flask_app.test_request_context():
        utils.clear_harvest_source(id)
    click.secho('Cleared harvest source: {0}'.format(id), fg='green')

@source.command()
@click.argument(u'id', metavar=u'SOURCE_ID_OR_NAME', required=False)
@click.pass_context
def clear_history(ctx, id):
    """If no source id is given the history for all harvest sources
    (maximum is 1000) will be cleared.

    Clears all jobs and objects related to a harvest source, but keeps
    the source itself.  The datasets imported from the harvest source
    will NOT be deleted!!!  If a source id is given, it only clears
    the history of the harvest source with the given source id.

    """
    flask_app = ctx.meta['flask_app']

    with flask_app.test_request_context():
        result = utils.clear_harvest_source_history(id)
    click.secho(result, fg='green')


@harvester.command()
@click.argument('all', required=False)
@click.pass_context
def sources(ctx, all):
    """Lists harvest sources.

    If 'all' is defined, it also shows the Inactive sources

    """
    flask_app = ctx.meta['flask_app']

    with flask_app.test_request_context():
        result = utils.list_sources(bool(all))
    click.echo(result)


@harvester.command()
@click.argument('id', metavar='SOURCE_ID_OR_NAME')
@click.pass_context
def job(ctx, id):
    """Create new harvest job and runs it (puts it on the gather queue).

    """
    flask_app = ctx.meta['flask_app']
    with flask_app.test_request_context():
        try:
            result = utils.create_job(id)
        except HarvestJobExists as e:
            tk.error_shout(e)
            ctx.abort()
    click.echo(result)

@harvester.command()
@click.pass_context
def jobs(ctx):
    """Lists harvest jobs.

    """
    flask_app = ctx.meta['flask_app']
    with flask_app.test_request_context():
        result = utils.list_jobs()
    click.echo(result)


@harvester.command()
@click.argument('id', metavar='SOURCE_OR_JOB_ID')
@click.pass_context
def job_abort(ctx, id):
    """Marks a job as "Aborted" so that the source can be restarted afresh.

    It ensures that the job's harvest objects status are also marked
    finished. You should ensure that neither the job nor its objects
    are currently in the gather/fetch queues.

    """
    flask_app = ctx.meta['flask_app']
    with flask_app.test_request_context():
        try:
            result = utils.abort_job(id)
        except tk.ObjectNotFound as e:
            tk.error_shout(u'Job not found.')
            ctx.abort()

    click.echo(result)

@harvester.command()
def purge_queues():
    """Removes all jobs from fetch and gather queue.
    """
    utils.purge_queues()


@harvester.command()
def gather_consumer():
    """Starts the consumer for the gathering queue.

    """
    utils.gather_consumer()

@harvester.command()
def fetch_consumer():
    """Starts the consumer for the fetching queue.

    """
    utils.fetch_consumer()


@harvester.command()
@click.pass_context
def run(ctx):
    """Starts any harvest jobs that have been created by putting them onto
    the gather queue.

    Also checks running jobs - if finished it changes their status to
    Finished.

    """
    flask_app = ctx.meta['flask_app']
    with flask_app.test_request_context():
        utils.run_harvester()

@harvester.command()
@click.pass_context
@click.argument('id', metavar='SOURCE_ID_OR_NAME')
def run_test(ctx, id):
    """Runs a harvest - for testing only.

    This does all the stages of the harvest (creates job, gather,
    fetch, import) without involving the web UI or the queue
    backends. This is useful for testing a harvester without having to
    fire up gather/fetch_consumer processes, as is done in production.

    """
    flask_app = ctx.meta['flask_app']
    with flask_app.test_request_context():
        utils.run_test_harvester(id)


class Harvester(object):
    '''Harvests remotely mastered metadata
    Usage:



      harvester clean_harvest_log
        - Clean-up mechanism for the harvest log table.
          You can configure the time frame through the configuration
          parameter `ckan.harvest.log_timeframe`. The default time frame is 30 days

      harvester [-j] [-o|-g|-p {id/guid}] [--segments={segments}] import [{source-id}]
        - perform the import stage with the last fetched objects, for a certain
          source or a single harvest object. Please note that no objects will
          be fetched from the remote server. It will only affect the objects
          already present in the database.

          To import a particular harvest source, specify its id as an argument.
          To import a particular harvest object use the -o option.
          To import a particular guid use the -g option.
          To import a particular package use the -p option.

          You will need to specify the -j flag in cases where the datasets are
          not yet created (e.g. first harvest, or all previous harvests have
          failed)

          The --segments flag allows to define a string containing hex digits that represent which of
          the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f

      harvester job-all
        - create new harvest jobs for all active sources.

      harvester reindex
        - reindexes the harvest source datasets

    The commands should be run from the ckanext-harvest directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster harvester sources --config=../ckan/development.ini

    '''

    def __init__(self, name):

        super(Harvester, self).__init__(name)

        self.parser.add_option('-j', '--no-join-datasets', dest='no_join_datasets',
                               action='store_true', default=False, help='Do not join harvest objects to existing datasets')

        self.parser.add_option('-o', '--harvest-object-id', dest='harvest_object_id',
                               default=False, help='Id of the harvest object to which perform the import stage')

        self.parser.add_option('-p', '--package-id', dest='package_id',
                               default=False, help='Id of the package whose harvest object to perform the import stage for')

        self.parser.add_option('-g', '--guid', dest='guid',
                               default=False, help='Guid of the harvest object to which perform the import stage for')

        self.parser.add_option('--segments', dest='segments',
                               default=False, help='''A string containing hex digits that represent which of
 the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f''')

    def command(self):
        self._load_config()


        if cmd == 'import':
            self.initdb()
            self.import_stage()
        elif cmd == 'job-all':
            self.create_harvest_job_all()
        elif cmd == 'harvesters-info':
            harvesters_info = tk.get_action('harvesters_info_show')()
            pprint(harvesters_info)
        elif cmd == 'reindex':
            self.reindex()
        elif cmd == 'clean_harvest_log':
            self.clean_harvest_log()
        else:
            print('Command {0} not recognized'.format(cmd))



    def import_stage(self):

        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
            context = {'model': model, 'session': model.Session,
                       'user': _admin_user()['name']}
            source = tk.get_action('harvest_source_show')(
                context, {'id': source_id_or_name})
            source_id = source['id']
        else:
            source_id = None

        context = {'model': model, 'session': model.Session,
                   'user': _admin_user()['name'],
                   'join_datasets': not self.options.no_join_datasets,
                   'segments': self.options.segments}

        objs_count = tk.get_action('harvest_objects_import')(context, {
                'source_id': source_id,
                'harvest_object_id': self.options.harvest_object_id,
                'package_id': self.options.package_id,
                'guid': self.options.guid,
                })

        print('{0} objects reimported'.format(objs_count))

    def create_harvest_job_all(self):
        context = {'model': model, 'user': _admin_user()['name'], 'session': model.Session}
        jobs = tk.get_action('harvest_job_create_all')(context, {})
        print('Created {0} new harvest jobs'.format(len(jobs)))

    def reindex(self):
        context = {'model': model, 'user': _admin_user()['name']}
        tk.get_action('harvest_sources_reindex')(context, {})


    def clean_harvest_log(self):
        from datetime import datetime, timedelta
        from ckantoolkit import config
        from ckanext.harvest.model import clean_harvest_log

        # Log time frame - in days
        log_timeframe = tk.asint(config.get('ckan.harvest.log_timeframe', 30))
        condition = datetime.utcnow() - timedelta(days=log_timeframe)

        # Delete logs older then the given date
        clean_harvest_log(condition=condition)

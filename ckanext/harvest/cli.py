# -*- coding: utf-8 -*-

from __future__ import print_function

import ckantoolkit as tk
import click

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
    click.secho(u"DB tables created", fg=u"green")


@harvester.group()
def source():
    """Manage harvest sources
    """
    pass


@source.command()
@click.argument(u"name")
@click.argument(u"url")
@click.argument(u"type")
@click.argument(u"title", required=False)
@click.argument(u"active", type=tk.asbool, default=True)
@click.argument(u"owner_org", required=False)
@click.argument(u"frequency", default=u"MANUAL")
@click.argument(u"config", required=False)
def create(name, url, type, title, active, owner_org, frequency, config):
    """Create new harvest source.
    """
    try:
        result = utils.create_harvest_source(
            name, url, type, title, active, owner_org, frequency, config
        )
    except tk.ValidationError as e:
        tk.error_shout(u"Validation error:")
        for field, err in e.error_summary.items():
            tk.error_shout("\t{}: {}".format(field, err))
        raise click.Abort()
    click.echo(result)


@source.command()
@click.argument(u"id", metavar=u"SOURCE_ID_OR_NAME")
@click.pass_context
def show(ctx, id):
    """Shows a harvest source.
    """
    flask_app = ctx.meta["flask_app"]

    try:
        with flask_app.test_request_context():
            result = utils.show_harvest_source(id)
    except tk.ObjectNotFound as e:
        tk.error_shout(u"Source <{}> not found.".format(id))
        raise click.Abort()
    click.echo(result)


@source.command()
@click.argument(u"id", metavar=u"SOURCE_ID_OR_NAME")
@click.pass_context
def remove(ctx, id):
    """Remove (deactivate) a harvester source, whilst leaving any related
    datasets, jobs and objects.

    """
    flask_app = ctx.meta["flask_app"]

    with flask_app.test_request_context():
        utils.remove_harvest_source(id)
    click.secho("Removed harvest source: {0}".format(id), fg="green")


@source.command()
@click.argument(u"id", metavar=u"SOURCE_ID_OR_NAME")
@click.pass_context
def clear(ctx, id):
    """Clears all datasets, jobs and objects related to a harvest source,
    but keeps the source itself.

    """
    flask_app = ctx.meta["flask_app"]

    with flask_app.test_request_context():
        utils.clear_harvest_source(id)
    click.secho("Cleared harvest source: {0}".format(id), fg="green")


@source.command()
@click.argument(u"id", metavar=u"SOURCE_ID_OR_NAME", required=False)
@click.pass_context
def clear_history(ctx, id):
    """If no source id is given the history for all harvest sources
    (maximum is 1000) will be cleared.

    Clears all jobs and objects related to a harvest source, but keeps
    the source itself.  The datasets imported from the harvest source
    will NOT be deleted!!!  If a source id is given, it only clears
    the history of the harvest source with the given source id.

    """
    flask_app = ctx.meta["flask_app"]

    with flask_app.test_request_context():
        result = utils.clear_harvest_source_history(id)
    click.secho(result, fg="green")


@harvester.command()
@click.argument("all", required=False)
@click.pass_context
def sources(ctx, all):
    """Lists harvest sources.

    If 'all' is defined, it also shows the Inactive sources

    """
    flask_app = ctx.meta["flask_app"]

    with flask_app.test_request_context():
        result = utils.list_sources(bool(all))
    click.echo(result)


@harvester.command()
@click.argument("id", metavar="SOURCE_ID_OR_NAME")
@click.pass_context
def job(ctx, id):
    """Create new harvest job and runs it (puts it on the gather queue).

    """
    flask_app = ctx.meta["flask_app"]
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
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        result = utils.list_jobs()
    click.echo(result)


@harvester.command()
@click.argument("id", metavar="SOURCE_OR_JOB_ID")
@click.pass_context
def job_abort(ctx, id):
    """Marks a job as "Aborted" so that the source can be restarted afresh.

    It ensures that the job's harvest objects status are also marked
    finished. You should ensure that neither the job nor its objects
    are currently in the gather/fetch queues.

    """
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        try:
            result = utils.abort_job(id)
        except tk.ObjectNotFound as e:
            tk.error_shout(u"Job not found.")
            ctx.abort()

    click.echo(result)


@harvester.command()
@click.argument("life_span", default=False, required=False)
@click.option(
    "-i",
    "--include",
    default=False,
    help="""If source_id provided as included, then only it's failed jobs will be aborted.
    You can use comma as a separator to provide multiple source_id's""",
)
@click.option(
    "-e",
    "--exclude",
    default=False,
    help="""If source_id provided as excluded, all sources failed jobs, except for that
    will be aborted. You can use comma as a separator to provide multiple source_id's""",
)
@click.pass_context
def abort_failed_jobs(ctx, life_span, include, exclude):
    """Abort all jobs which are in a "limbo state" where the job has
    run with errors but the harvester run command will not mark it
    as finished, and therefore you cannot run another job.
    """
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        result = utils.abort_failed_jobs(life_span, include, exclude)
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
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.run_harvester()


@harvester.command()
@click.pass_context
@click.argument("id", metavar="SOURCE_ID_OR_NAME")
@click.argument("force-import", required=False, metavar="GUID")
def run_test(ctx, id, force_import=None):
    """Runs a harvest - for testing only.

    This does all the stages of the harvest (creates job, gather,
    fetch, import) without involving the web UI or the queue
    backends. This is useful for testing a harvester without having to
    fire up gather/fetch_consumer processes, as is done in production.

    """
    if force_import:
        force_import_val = force_import.split('=')[-1]
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.run_test_harvester(id, force_import_val)


@harvester.command("import")
@click.pass_context
@click.argument("id", metavar="SOURCE_ID_OR_NAME", required=False)
@click.option(
    "-j",
    "--no-join-datasets",
    is_flag=True,
    help="Do not join harvest objects to existing datasets",
)
@click.option(
    "-o",
    "--harvest-object-id",
    help="Id of the harvest object to which perform the import stage",
)
@click.option(
    "-p",
    "--package-id",
    help="Id of the package whose harvest object to perform the import stage for",
)
@click.option(
    "-g",
    "--guid",
    help="Guid of the harvest object to which perform the import stage for",
)
@click.option(
    "--segments",
    help="""A string containing hex digits that represent which of
 the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f""",
)
def import_stage(
    ctx, id, no_join_datasets, harvest_object_id, guid, package_id, segments
):
    """Perform the import stage with the last fetched objects, for a
    certain source or a single harvest object.

    Please note that no objects will be fetched from the remote
    server. It will only affect the objects already present in the
    database.

    To import a particular harvest source, specify its id as an argument.
    To import a particular harvest object use the -o option.
    To import a particular guid use the -g option.
    To import a particular package use the -p option.

    You will need to specify the -j flag in cases where the datasets
    are not yet created (e.g. first harvest, or all previous harvests
    have failed)

    The --segments flag allows to define a string containing hex
    digits that represent which of the 16 harvest object segments to
    import. e.g. 15af will run segments 1,5,a,f

    """
    ctx.invoke(initdb)
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        try:
            utils.import_stage(
                id,
                no_join_datasets,
                harvest_object_id,
                guid,
                package_id,
                segments,
            )
        except tk.ObjectNotFound as e:
            tk.error_shout(u"Source <{}> not found.".format(id))


@harvester.command()
@click.pass_context
def clean_harvest_log(ctx):
    """Clean-up mechanism for the harvest log table.

    You can configure the time frame through the configuration
    parameter `ckan.harvest.log_timeframe`. The default time frame is 30
    days

    """
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.clean_harvest_log()


@harvester.command("job-all")
@click.pass_context
def job_all(ctx):
    """Create new harvest jobs for all active sources.

    """
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        result = utils.job_all()
    click.echo(result)


@harvester.command()
@click.pass_context
def reindex(ctx):
    """Reindexes the harvest source datasets.

    """
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        utils.reindex()


@harvester.command("harvesters_info")
@click.pass_context
def harvesters_info(ctx):
    """

    """
    flask_app = ctx.meta["flask_app"]
    with flask_app.test_request_context():
        result = utils.harvesters_info()

    click.echo(result)

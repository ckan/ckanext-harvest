from __future__ import print_function

import sys

import six

from ckan import model
from ckan.logic import get_action, ValidationError

from ckantoolkit import CkanCommand

import ckanext.harvest.utils as utils


class Harvester(CkanCommand):
    """Harvests remotely mastered metadata

    Usage:

      harvester initdb
        - Creates the necessary tables in the database

      harvester source {name} {url} {type} [{title}] [{active}] [{owner_org}] [{frequency}] [{config}]
        - create new harvest source

      harvester source {source-id/name}
        - shows a harvest source

      harvester rmsource {source-id/name}
        - remove (deactivate) a harvester source, whilst leaving any related
          datasets, jobs and objects

      harvester clearsource {source-id/name}
        - clears all datasets, jobs and objects related to a harvest source,
          but keeps the source itself

      harvester clearsource_history [{source-id}]
        - If no source id is given the history for all harvest sources (maximum is 1000) will be cleared.
          Clears all jobs and objects related to a harvest source, but keeps the source itself.
          The datasets imported from the harvest source will NOT be deleted!!!
          If a source id is given, it only clears the history of the harvest source with the given source id.

      harvester sources [all]
        - lists harvest sources
          If 'all' is defined, it also shows the Inactive sources

      harvester job {source-id/name}
        - create new harvest job and runs it (puts it on the gather queue)

      harvester jobs
        - lists harvest jobs

      harvester job_abort {source-id/source-name/obj-id}
        - marks a job as "Aborted" so that the source can be restarted afresh.
          It ensures that the job's harvest objects status are also marked
          finished. You should ensure that neither the job nor its objects are
          currently in the gather/fetch queues.

      harvester run
        - starts any harvest jobs that have been created by putting them onto
          the gather queue. Also checks running jobs - if finished it
          changes their status to Finished.

      harvester run_test {source-id/name}
        - runs a harvest - for testing only.
          This does all the stages of the harvest (creates job, gather, fetch,
          import) without involving the web UI or the queue backends. This is
          useful for testing a harvester without having to fire up
          gather/fetch_consumer processes, as is done in production.

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester purge_queues
        - removes all jobs from fetch and gather queue

      harvester abort_failed_jobs {job_life_span} [--include={source_id}] [--exclude={source_id}]
        - abort all jobs which are in a "limbo state" where the job has
          run with errors but the harvester run command will not mark it
          as finished, and therefore you cannot run another job.

          job_life_span determines from what moment
          the job must be considered as failed

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

    """

    summary = __doc__.split("\n")[0]
    usage = __doc__
    max_args = 9
    min_args = 0

    def __init__(self, name):

        super(Harvester, self).__init__(name)

        self.parser.add_option(
            "-j",
            "--no-join-datasets",
            dest="no_join_datasets",
            action="store_true",
            default=False,
            help="Do not join harvest objects to existing datasets",
        )

        self.parser.add_option(
            "-o",
            "--harvest-object-id",
            dest="harvest_object_id",
            default=False,
            help="Id of the harvest object to which perform the import stage",
        )

        self.parser.add_option(
            "-p",
            "--package-id",
            dest="package_id",
            default=False,
            help="Id of the package whose harvest object to perform the import stage for",
        )

        self.parser.add_option(
            "-g",
            "--guid",
            dest="guid",
            default=False,
            help="Guid of the harvest object to which perform the import stage for",
        )

        self.parser.add_option(
            "--segments",
            dest="segments",
            default=False,
            help="""A string containing hex digits that represent which of
 the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f""",
        )

        self.parser.add_option(
            "-i",
            "--include",
            dest="include_sources",
            default=False,
            help="""If source_id provided as included, then only it's failed jobs will be aborted.
            You can use comma as a separator to provide multiple source_id's""",
        )

        self.parser.add_option(
            "-e",
            "--exclude",
            dest="exclude_sources",
            default=False,
            help="""If source_id provided as excluded, all sources failed jobs, except for that
            will be aborted. You can use comma as a separator to provide multiple source_id's""",
        )

    def command(self):
        self._load_config()

        # We'll need a sysadmin user to perform most of the actions
        # We will use the sysadmin site user (named as the site_id)
        context = {
            "model": model,
            "session": model.Session,
            "ignore_auth": True,
        }
        self.admin_user = get_action("get_site_user")(context, {})

        print("")

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        if cmd == "source":
            if len(self.args) > 2:
                self.create_harvest_source()
            else:
                self.show_harvest_source()
        elif cmd == "rmsource":
            self.remove_harvest_source()
        elif cmd == "clearsource":
            self.clear_harvest_source()
        elif cmd == "clearsource_history":
            self.clear_harvest_source_history()
        elif cmd == "sources":
            self.list_harvest_sources()
        elif cmd == "job":
            self.create_harvest_job()
        elif cmd == "jobs":
            self.list_harvest_jobs()
        elif cmd == "job_abort":
            self.job_abort()
        elif cmd == "run":
            self.run_harvester()
        elif cmd == "run_test":
            self.run_test_harvest()
        elif cmd == "gather_consumer":
            utils.gather_consumer()
        elif cmd == "fetch_consumer":
            utils.fetch_consumer()
        elif cmd == "purge_queues":
            self.purge_queues()
        elif cmd == "abort_failed_jobs":
            self.abort_failed_jobs()
        elif cmd == "initdb":
            self.initdb()
        elif cmd == "import":
            self.initdb()
            self.import_stage()
        elif cmd == "job-all":
            self.create_harvest_job_all()
        elif cmd == "harvesters-info":
            print(utils.harvesters_info())
        elif cmd == "reindex":
            self.reindex()
        elif cmd == "clean_harvest_log":
            self.clean_harvest_log()
        else:
            print("Command {0} not recognized".format(cmd))

    def _load_config(self):
        super(Harvester, self)._load_config()

    def initdb(self):
        utils.initdb()
        print("DB tables created")

    def create_harvest_source(self):

        if len(self.args) >= 2:
            name = six.text_type(self.args[1])
        else:
            print("Please provide a source name")
            sys.exit(1)
        if len(self.args) >= 3:
            url = six.text_type(self.args[2])
        else:
            print("Please provide a source URL")
            sys.exit(1)
        if len(self.args) >= 4:
            type = six.text_type(self.args[3])
        else:
            print("Please provide a source type")
            sys.exit(1)

        if len(self.args) >= 5:
            title = six.text_type(self.args[4])
        else:
            title = None
        if len(self.args) >= 6:
            active = not (
                self.args[5].lower() == "false" or self.args[5] == "0"
            )
        else:
            active = True
        if len(self.args) >= 7:
            owner_org = six.text_type(self.args[6])
        else:
            owner_org = None
        if len(self.args) >= 8:
            frequency = six.text_type(self.args[7])
            if not frequency:
                frequency = "MANUAL"
        else:
            frequency = "MANUAL"
        if len(self.args) >= 9:
            source_config = six.text_type(self.args[8])
        else:
            source_config = None
        try:
            result = utils.create_harvest_source(
                name, url, type, title, active, owner_org, frequency, source_config
            )
        except ValidationError as e:
            print("An error occurred:")
            print(str(e.error_dict))
            raise e

        print(result)

    def clear_harvest_source_history(self):
        source_id = None
        if len(self.args) >= 2:
            source_id = six.text_type(self.args[1])

        print(utils.clear_harvest_source_history(source_id))

    def show_harvest_source(self):

        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print("Please provide a source name")
            sys.exit(1)
        print(utils.show_harvest_source(source_id_or_name))

    def remove_harvest_source(self):
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print("Please provide a source id")
            sys.exit(1)
        utils.remove_harvest_source(source_id_or_name)

    def clear_harvest_source(self):
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print("Please provide a source id")
            sys.exit(1)
        utils.clear_harvest_source(source_id_or_name)

    def list_harvest_sources(self):
        if len(self.args) >= 2 and self.args[1] == "all":
            all = True
        else:
            all = False

        print(utils.list_sources(all))

    def create_harvest_job(self):
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print("Please provide a source id")
            sys.exit(1)
        print(utils.create_job(source_id_or_name))

    def list_harvest_jobs(self):
        print(utils.list_jobs())

    def job_abort(self):
        if len(self.args) >= 2:
            job_or_source_id_or_name = six.text_type(self.args[1])
        else:
            print("Please provide a job id or source name/id")
            sys.exit(1)
        print(utils.abort_job(job_or_source_id_or_name))

    def run_harvester(self):
        utils.run_harvester()

    def run_test_harvest(self):
        # Determine the source
        force_import = None
        if len(self.args) >= 2:
            if len(self.args) >= 3 and self.args[2].startswith('force-import='):
                force_import = self.args[2].split('=')[-1]
            source_id_or_name = six.text_type(self.args[1])
        else:
            print("Please provide a source id")
            sys.exit(1)

        utils.run_test_harvester(source_id_or_name, force_import)

    def import_stage(self):

        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
            context = {
                "model": model,
                "session": model.Session,
                "user": self.admin_user["name"],
            }
            source = get_action("harvest_source_show")(
                context, {"id": source_id_or_name}
            )
            source_id = source["id"]
        else:
            source_id = None
        utils.import_stage(
            source_id,
            self.options.no_join_datasets,
            self.options.harvest_object_id,
            self.options.guid,
            self.options.package_id,
            self.options.segments,
        )

    def create_harvest_job_all(self):
        print(utils.job_all())

    def reindex(self):
        utils.reindex()

    def purge_queues(self):
        utils.purge_queues()

    def clean_harvest_log(self):
        utils.clean_harvest_log()

    def abort_failed_jobs(self):
        job_life_span = False
        if len(self.args) >= 2:
            job_life_span = six.text_type(self.args[1])

        utils.abort_failed_jobs(
            job_life_span,
            include=self.options.include_sources,
            exclude=self.options.exclude_sources
        )

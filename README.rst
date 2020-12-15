=============================================
ckanext-harvest - Remote harvesting extension
=============================================

.. image:: https://github.com/ckan/ckanext-harvest/workflows/Tests/badge.svg?branch=master
    :target: https://github.com/ckan/ckanext-harvest/actions

This extension provides a common harvesting framework for ckan extensions
and adds a CLI and a WUI to CKAN to manage harvesting sources and jobs.


Installation
============

This extension requires CKAN v2.0 or later on both the CKAN it is installed
into and the CKANs it harvests. However you are unlikely to encounter a CKAN
running a version lower than 2.0.

1. The harvest extension can use two different backends. You can choose whichever
   you prefer depending on your needs, but Redis has been found to be more stable
   and reliable so it is the recommended one:

   * `Redis <http://redis.io/>`_ (recommended): To install it, run::

      sudo apt-get update
      sudo apt-get install redis-server

     On your CKAN configuration file, add in the `[app:main]` section::

      ckan.harvest.mq.type = redis

   * `RabbitMQ <http://www.rabbitmq.com/>`_: To install it, run::

      sudo apt-get update
      sudo apt-get install rabbitmq-server

     On your CKAN configuration file, add in the `[app:main]` section::

      ckan.harvest.mq.type = amqp

2. Activate your CKAN virtual environment, for example::

     $ . /usr/lib/ckan/default/bin/activate

3. Install the ckanext-harvest Python package into your virtual environment::

     (pyenv) $ pip install -e git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest

4. Install the python modules required by the extension (adjusting the path according to where ckanext-harvest was installed in the previous step)::

     (pyenv) $ cd /usr/lib/ckan/default/src/ckanext-harvest/
     (pyenv) $ pip install -r pip-requirements.txt

5. Make sure the CKAN configuration ini file contains the harvest main plugin, as
   well as the harvester for CKAN instances if you need it (included with the extension)::

     ckan.plugins = harvest ckan_harvester

6. If you haven't done it yet on the previous step, define the backend that you
   are using with the ``ckan.harvest.mq.type`` option in the `[app:main]` section (it defaults to ``amqp``)::

     ckan.harvest.mq.type = redis


There are a number of configuration options available for the backends. These don't need to be modified at all if you are using the default Redis or RabbitMQ install (step 1). However you may wish to add them with custom options to the into the CKAN config file the `[app:main]` section. The list below shows the available options and their default values:

* Redis:
    - ``ckan.harvest.mq.hostname`` (localhost)
    - ``ckan.harvest.mq.port`` (6379)
    - ``ckan.harvest.mq.redis_db`` (0)
    - ``ckan.harvest.mq.password`` (None)

* RabbitMQ:
    - ``ckan.harvest.mq.user_id`` (guest)
    - ``ckan.harvest.mq.password`` (guest)
    - ``ckan.harvest.mq.hostname`` (localhost)
    - ``ckan.harvest.mq.port`` (5672)
    - ``ckan.harvest.mq.virtual_host`` (/)


**Note**: it is safe to use the same backend server (either Redis or RabbitMQ)
for different CKAN instances, as long as they have different site ids. The ``ckan.site_id``
config option (or ``default``) will be used to namespace the relevant things:

* On RabbitMQ it will be used to name the queues used, eg ``ckan.harvest.site1.gather`` and
  ``ckan.harvest.site1.fetch``.

* On Redis, it will namespace the keys used, so only the relevant instance gets them, eg
  ``site1:harvest_job_id``,  ``site1:harvest_object__id:804f114a-8f68-4e7c-b124-3eb00f66202f``


Configuration
=============

Run the following command to create the necessary tables in the database (ensuring the pyenv is activated):

ON CKAN >= 2.9::

    (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester initdb

ON CKAN <= 2.8::

    (pyenv) $ paster --plugin=ckanext-harvest harvester initdb --config=/etc/ckan/default/production.ini

Finally, restart CKAN to have the changes take effect::

    sudo service apache2 restart

After installation, the harvest source listing should be available under /harvest, eg::

    http://localhost/harvest


Database logger configuration(optional)
=======================================

1. Logging to the database is disabled by default. If you want your ckan harvest logs
   to be exposed to the CKAN API you need to properly configure the logger
   with the following configuration parameter::

     ckan.harvest.log_scope = 0

   * -1 - Do not log in the database - DEFAULT
   *  0 - Log everything
   *  1 - model, logic.action, logic.validators, harvesters
   *  2 - model, logic.action, logic.validators
   *  3 - model, logic.action
   *  4 - logic.action
   *  5 - model
   *  6 - plugin
   *  7 - harvesters

2. Setup time frame (in days) for the clean-up mechanism with the following config parameter (in the `[app:main]` section)::

     ckan.harvest.log_timeframe = 10

   If no value is present the default is 30 days.

3. Setup log level for the database logger::

     ckan.harvest.log_level = info

   If no log level is set the default is ``debug``.


**API Usage**

You can access CKAN harvest logs via the API::

    $ curl {ckan_url}/api/3/action/harvest_log_list

Replace {ckan_url} with the url from your CKAN instance.

Allowed parameters are:

* ``level`` (filter log records by level)

* ``limit`` (used for pagination)

* ``offset`` (used for pagination)

e.g. Fetch all logs with log level INFO::

    $ curl {ckan_url}/api/3/action/harvest_log_list?level=info

    {
      "help":"http://127.0.0.1:5000/api/3/action/help_show?name=harvest_log_list",

      "success":true,

      "result": [{"content":"Sent job aa987717-2316-4e47-b0f2-cbddfb4c4dfc to the gather queue","level":"INFO","created":"2016-06-03 10:59:40.961657"}, {"content":"Sent job aa987717-2316-4e47-b0f2-cbddfb4c4dfc to the gather queue","level":"INFO","created":"2016-06-03 10:59:40.951548"}]

    }


Dataset name generation configuration (optional)
================================================

If the dataset name is created based on the title, duplicate names may occur.
To avoid this, a suffix is appended to the name if it already exists.

You can configure the default behaviour in your production.ini:

    ckanext.harvest.default_dataset_name_append = number-sequence

or

    ckanext.harvest.default_dataset_name_append = random-hex

If you don't specify this setting, the default will be number-sequence.


Send error mails when harvesting fails (optional)
=================================================

If you want to send an email when a **Harvest Job fails**, you can set the following configuration option in the ini file:

    ckan.harvest.status_mail.errored = True

If you want to send an email when **completed Harvest Jobs finish** (whether or not it failed), you can set the following configuration option in the ini file:

    ckan.harvest.status_mail.all = True

That way, all CKAN Users who are declared as Sysadmins will receive the Error emails at their configured email address. If the Harvest-Source of the failing Harvest-Job belongs to an organization, the error-mail will also be sent to the organization-members who have the admin-role if their E-Mail is configured.

If you don't specify this setting, the default will be False.


Set a timeout for a harvest job (optional)
================================================

IF you want to set a timeout for harvest jobs, you can add this configuration option to the ini file:

    ckan.harvest.timeout = 1440

The timeout value is in minutes, so 1440 represents 24 hours. 
Any jobs which are timed out will create an error message for the user to see.

If you don't specify this setting, the default will be False and there will be no timeout on harvest jobs.
This timeout value is compared to the completion time of the last object in the job.


Command line interface
======================

The following operations can be run from the command line as described underneath::

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
        - If no source id is given the history for all harvest sources (maximum is 1000)
          will be cleared.
          Clears all jobs and objects related to a harvest source, but keeps the source
          itself. The datasets imported from the harvest source will **NOT** be deleted!!!
          If a source id is given, it only clears the history of the harvest source with
          the given source id.

      harvester sources [all]
        - lists harvest sources
          If 'all' is defined, it also shows the Inactive sources

      harvester job {source-id/name}
        - create new harvest job

      harvester jobs
        - lists harvest jobs

      harvester job_abort {source-id/name}
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
          
      harvester run_test {source-id/name} force-import=guid1,guid2...
        - In order to force an import of particular datasets, useful to 
          target a dataset for dev purposes or when forcing imports on other environments.

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester purge_queues
        - removes all jobs from fetch and gather queue
          WARNING: if using Redis, this command purges all data in the current
          Redis database

      harvester clean_harvest_log
        - Clean-up mechanism for the harvest log table.
          You can configure the time frame through the configuration
          parameter 'ckan.harvest.log_timeframe'. The default time frame is 30 days

      harvester [-j] [-o] [--segments={segments}] import [{source-id}]
        - perform the import stage with the last fetched objects, for a certain
          source or a single harvest object. Please note that no objects will
          be fetched from the remote server. It will only affect the objects
          already present in the database.

          To import a particular harvest source, specify its id as an argument.
          To import a particular harvest object use the -o option.
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

The commands should be run with the pyenv activated and refer to your CKAN configuration file:

ON CKAN >= 2.9::

    (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester --help

    (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester sources

ON CKAN <= 2.8::

      (pyenv) $ paster --plugin=ckanext-harvest harvester sources --config=/etc/ckan/default/production.ini

Authorization
=============

Starting from CKAN 2.0, harvest sources behave exactly the same as datasets
(they are actually internally implemented as a dataset type). That means they
can be searched and faceted, and that the same authorization rules can be
applied to them. The default authorization settings are based on organizations.

Have a look at the `Authorization <http://docs.ckan.org/en/latest/authorization.html>`_
documentation on CKAN core to see how to configure your instance depending on
your needs.

The CKAN harvester
===================

The plugin includes a harvester for remote CKAN instances. To use it, you need
to add the `ckan_harvester` plugin to your options file::

	ckan.plugins = harvest ckan_harvester

After adding it, a 'CKAN' option should appear in the 'New harvest source' form.

The CKAN harvesters support a number of configuration options to control their
behaviour. Those need to be defined as a JSON object in the configuration form
field. The currently supported configuration options are:

*   api_version: You can force the harvester to use either version 1 or 2 of
    the CKAN API. Default is 2.

*   default_tags: A list of tags that will be added to all harvested datasets.
    Tags don't need to previously exist. This field takes a list of tag dicts
    (see example), which allows you to optinally specify a vocabulary.

*   default_groups: A list of group IDs or names to which the harvested datasets
    will be added to. The groups must exist.

*   default_extras: A dictionary of key value pairs that will be added to extras
    of the harvested datasets. You can use the following replacement strings,
    that will be replaced before creating or updating the datasets:

    * {dataset_id}
    * {harvest_source_id}
    * {harvest_source_url}   # Will be stripped of trailing forward slashes (/)
    * {harvest_source_title}
    * {harvest_job_id}
    * {harvest_object_id}

*   override_extras: Assign default extras even if they already exist in the
    remote dataset. Default is False (only non existing extras are added).

*   user: User who will run the harvesting process. Please note that this user
    needs to have permission for creating packages, and if default groups were
    defined, the user must have permission to assign packages to these groups.

*   api_key: If the remote CKAN instance has restricted access to the API, you
    can provide a CKAN API key, which will be sent in any request.

*   read_only: Create harvested packages in read-only mode. Only the user who
    performed the harvest (the one defined in the previous setting or the
    'harvest' sysadmin) will be able to edit and administer the packages
    created from this harvesting source. Logged in users and visitors will be
    only able to read them.

*   force_all: By default, after the first harvesting, the harvester will gather
    only the modified packages from the remote site since the last harvesting.
    Setting this property to true will force the harvester to gather all remote
    packages regardless of the modification date. Default is False.

*   remote_groups: By default, remote groups are ignored. Setting this property
    enables the harvester to import the remote groups. There are two alternatives.
    Setting it to 'only_local' will just import groups which name/id is already
    present in the local CKAN. Setting it to 'create' will make an attempt to
    create the groups by copying the details from the remote CKAN.

*   remote_orgs: By default, remote organizations are ignored. Setting this property
    enables the harvester to import remote organizations. There are two alternatives.
    Setting it to 'only_local' will just import organizations which id is already
    present in the local CKAN. Setting it to 'create' will make an attempt to
    create the organizations by copying the details from the remote CKAN.

*   clean_tags: By default, tags are not stripped of accent characters, spaces and
    capital letters for display. If this option is set to True, accent characters
    will be replaced by their ascii equivalents, capital letters replaced by
    lower-case ones, and spaces replaced with dashes. Setting this option to False
    gives the same effect as leaving it unset.

*   organizations_filter_include: This configuration option allows you to specify
    a list of remote organization names (e.g. "arkansas-gov" is the name for
    organization http://catalog.data.gov/organization/arkansas-gov ). If this
    property has a value then only datasets that are in one of these organizations
    will be harvested. All other datasets will be skipped. Only one of
    organizations_filter_include or organizations_filter_exclude should be
    configured.

*   organizations_filter_exclude: This configuration option allows you to specify
    a list of remote organization names (e.g. "arkansas-gov" is the name for
    organization http://catalog.data.gov/organization/arkansas-gov ). If this
    property is set then all datasets from the remote source will be harvested
    unless it belongs to one of the organizations in this option. Only one of
    organizations_filter_exclude or organizations_filter_include should be
    configured.

*   groups_filter_include: Exactly the same as organizations_filter_include but for
    groups.

*   groups_filter_exclude: Exactly the same as organizations_filter_exclude but for
    groups.


Here is an example of a configuration object (the one that must be entered in
the configuration field)::

    {
     "api_version": 1,
     "default_tags": [{"name": "geo"}, {"name": "namibia"}],
     "default_groups": ["science", "spend-data"],
     "default_extras": {"encoding":"utf8", "harvest_url": "{harvest_source_url}/dataset/{dataset_id}"},
     "override_extras": true,
     "organizations_filter_include": [],
     "organizations_filter_exclude": ["remote-organization"],
     "user":"harverster-user",
     "api_key":"<REMOTE_API_KEY>",
     "read_only": true,
     "remote_groups": "only_local",
     "remote_orgs": "create"
    }


Plugins can extend the default CKAN harvester and implement the ``modify_package_dict`` in order to
modify the dataset dict generated by the harvester just before it is actually created or updated. For instance,
they might want to add or delete certain fields, or fire additional tasks based on the metadata fields.

Plugins will get the dataset dict including any processig described above (eg with the correct groups assigned,
replacement strings applied, etc). It will also be passed the harvest object, which contains the original, unmodified
dataset dict in the ``content`` property.

This is a simple example::

    from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

    class MySiteCKANHarvester(CKANHarvester):

        def modify_package_dict(self, package_dict, harvest_object):

            # Set a default custom field

            package_dict['remote_harvest'] = True

            # Add tags
            package_dict['tags'].append({'name': 'sdi'})

            return package_dict

Remember to register your custom harvester plugin in your extension ``setup.py`` file, and load the plugin in the config in file afterwards::

        # setup.py

        entry_points='''
            [ckan.plugins]
            my_site=ckanext.my_site.plugin:MySitePlugin
            my_site_ckan_harvester=ckanext.my_site.harvesters:MySiteCKANHarvester
        '''


        # ini file
        ckan.plugins = ... my_site my_site_ckan_harvester


The harvesting interface
========================

Extensions can implement the harvester interface to perform harvesting
operations. The harvesting process takes place on three stages:

1. The **gather** stage compiles all the resource identifiers that need to
   be fetched in the next stage (e.g. in a CSW server, it will perform a
   `GetRecords` operation).

2. The **fetch** stage gets the contents of the remote objects and stores
   them in the database (e.g. in a CSW server, it will perform n
   `GetRecordById` operations).

3. The **import** stage performs any necessary actions on the fetched
   resource (generally creating a CKAN package, but it can be anything the
   extension needs).

Plugins willing to implement the harvesting interface must provide the
following methods::

    from ckan.plugins.core import SingletonPlugin, implements
    from ckanext.harvest.interfaces import IHarvester

    class MyHarvester(SingletonPlugin):
    '''
    A Test Harvester
    '''
    implements(IHarvester)

    def info(self):
        '''
        Harvesting implementations must provide this method, which will return
        a dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This
          will appear on the form as a guidance to the user.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        '''

    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''

    def get_original_url(self, harvest_object_id):
        '''

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''

    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its job. The HarvestObjects need a
              reference date with the last modified date for the resource, this
              may need to be set in a different stage depending on the type of
              source.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.
            - to abort the harvest, create a HarvestGatherError and raise an
              exception. Any created HarvestObjects will be deleted.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything is ok (ie the object should now be
              imported), "unchanged" if the object didn't need harvesting after
              all (ie no error, but don't continue to import stage) or False if
              there were errors.

        :param harvest_object: HarvestObject object
        :returns: True if successful, 'unchanged' if nothing to import after
                  all, False if not successful
        '''

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g.
              create, update or delete a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - setting the HarvestObject.package (if there is one)
            - setting the HarvestObject.current for this harvest:
               - True if successfully created/updated
               - False if successfully deleted
            - setting HarvestObject.current to False for previous harvest
              objects of this harvest source if the action was successful.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - creating the HarvestObject - Package relation (if necessary)
            - returning True if the action was done, "unchanged" if the object
              didn't need harvesting after all or False if there were errors.

        NB You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                  need harvesting after all or False if there were errors.
        '''


See the CKAN harvester for an example of how to implement the harvesting
interface:

* ckanext-harvest/ckanext/harvest/harvesters/ckanharvester.py

Here you can also find other examples of custom harvesters:

* https://github.com/ckan/ckanext-dcat/tree/master/ckanext/dcat/harvesters
* https://github.com/ckan/ckanext-spatial/tree/master/ckanext/spatial/harvesters

Running the harvest jobs
========================

There are two ways to run a harvest:

1. ``harvester run_test`` for the command-line, suitable for testing
2. ``harvester run`` used by the Web UI and scheduled runs

harvester run_test
------------------

You can run a harvester simply using the ``run_test`` command. This is handy
for running a harvest with one command in the console and see all the output
in-line. It runs the gather, fetch and import stages all in the same process.
You must ensure that you have pip installed ``dev-requirements.txt`` 
in ``/home/ckan/ckan/lib/default/src/ckanext-harvest`` before using the
``run_test`` command.
  
This is useful for developing a harvester because you can insert break-points
in your harvester, and rerun a harvest without having to restart the
gather_consumer and fetch_consumer processes each time. In addition, because it
doesn't use the queue backends it doesn't interfere with harvests of other
sources that may be going on in the background.

However running this way, if gather_stage, fetch_stage or import_stage raise an
exception, they are not caught, whereas with ``harvester run`` they are handled
slightly differently as they are called by queue.py. So when testing this
aspect its best to use ``harvester run``.

harvester run
-------------

When a harvest job is started by a user in the Web UI, or by a scheduled
harvest, the harvest is started by the ``harvester run`` command. This is the
normal method in production systems and scales well.

In this case, the harvesting extension uses two different queues: one that
handles the gathering and another one that handles the fetching and importing.
To start the consumers run the following command (make sure you have your
python environment activated):

ON CKAN >= 2.9::

      (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester gather_consumer

ON CKAN <= 2.8::

      (pyenv) $ paster --plugin=ckanext-harvest harvester gather_consumer --config=/etc/ckan/default/production.ini

On another terminal, run the following command:

ON CKAN >= 2.9::

      (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester fetch_consumer

ON CKAN <= 2.8::

      (pyenv) $ paster --plugin=ckanext-harvest harvester fetch_consumer --config=/etc/ckan/default/production.ini

Finally, on a third console, run the following command to start any
pending harvesting jobs:

ON CKAN >= 2.9::

      (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester run

ON CKAN <= 2.8::

      (pyenv) $ paster --plugin=ckanext-harvest harvester run --config=/etc/ckan/default/production.ini

The ``run`` command not only starts any pending harvesting jobs, but also
flags those that are finished, allowing new jobs to be created on that particular
source and refreshing the source statistics. That means that you will need to run
this command before being able to create a new job on a source that was being
harvested. (On a production site you will typically have a cron job that runs the
command regularly, see next section).

Occasionally you can find a harvesting job is in a "limbo state" where the job
has run with errors but the ``harvester run`` command will not mark it as
finished, and therefore you cannot run another job. This is due to particular
harvester not handling errors correctly e.g. during development. In this
circumstance, ensure that the gather & fetch consumers are running and have
nothing more to consume, and then run this abort command with the name or id of
the harvest source:

ON CKAN >= 2.9::

      (pyenv) $ ckan --config=/etc/ckan/default/ckan.ini harvester job_abort {source-id/name}

ON CKAN <= 2.8::

      (pyenv) $ paster --plugin=ckanext-harvest harvester job_abort {source-id/name} --config=/etc/ckan/default/production.ini


Setting up the harvesters on a production server
================================================

The previous approach works fine during development or debugging, but it is
not recommended for production servers. There are several possible ways of
setting up the harvesters, which will depend on your particular infrastructure
and needs. The bottom line is that the gather and fetch process should be kept
running somehow and then the run command should be run periodically to start
any pending jobs.

The following approach is the one generally used on CKAN deployments, and it
will probably suit most of the users. It uses Supervisor_, a tool to monitor
processes, and a cron job to run the harvest jobs, and it assumes that you
have already installed and configured the harvesting extension (See
`Installation` if not).

Note: It is recommended to run the harvest process from a non-root user
(generally the one you are running CKAN with). Replace the user `ckan` in the
following steps with the one you are using.

1. Install Supervisor::

       sudo apt-get update
       sudo apt-get install supervisor

   You can check if it is running with this command::

       ps aux | grep supervisord

   You should see a line similar to this one::

       root      9224  0.0  0.3  56420 12204 ?        Ss   15:52   0:00 /usr/bin/python /usr/bin/supervisord

2. Supervisor needs to have programs added to its configuration, which will
   describe the tasks that need to be monitored. This configuration files are
   stored in ``/etc/supervisor/conf.d``.

   Create a file named ``/etc/supervisor/conf.d/ckan_harvesting.conf``, and
   copy the following contents:

   ON CKAN >= 2.9::

        ; ===============================
        ; ckan harvester
        ; ===============================

        [program:ckan_gather_consumer]

        command=/usr/lib/ckan/default/bin/ckan --config=/etc/ckan/default/ckan.ini harvester gather_consumer

        ; user that owns virtual environment.
        user=ckan

        numprocs=1
        stdout_logfile=/var/log/ckan/std/gather_consumer.log
        stderr_logfile=/var/log/ckan/std/gather_consumer.log
        autostart=true
        autorestart=true
        startsecs=10

        [program:ckan_fetch_consumer]

        command=/usr/lib/ckan/default/bin/ckan --config=/etc/ckan/default/ckan.ini harvester fetch_consumer

        ; user that owns virtual environment.
        user=ckan

        numprocs=1
        stdout_logfile=/var/log/ckan/std/fetch_consumer.log
        stderr_logfile=/var/log/ckan/std/fetch_consumer.log
        autostart=true
        autorestart=true
        startsecs=10


   ON CKAN <= 2.8::


        ; ===============================
        ; ckan harvester
        ; ===============================

        [program:ckan_gather_consumer]

        command=/usr/lib/ckan/default/bin/paster --plugin=ckanext-harvest harvester gather_consumer --config=/etc/ckan/default/production.ini

        ; user that owns virtual environment.
        user=ckan

        numprocs=1
        stdout_logfile=/var/log/ckan/std/gather_consumer.log
        stderr_logfile=/var/log/ckan/std/gather_consumer.log
        autostart=true
        autorestart=true
        startsecs=10

        [program:ckan_fetch_consumer]

        command=/usr/lib/ckan/default/bin/paster --plugin=ckanext-harvest harvester fetch_consumer --config=/etc/ckan/default/production.ini

        ; user that owns virtual environment.
        user=ckan

        numprocs=1
        stdout_logfile=/var/log/ckan/std/fetch_consumer.log
        stderr_logfile=/var/log/ckan/std/fetch_consumer.log
        autostart=true
        autorestart=true
        startsecs=10


   There are a number of things that you will need to replace with your
   specific installation settings (the example above shows paths from a
   ckan instance installed via Debian packages):

   * command: The absolute path to the paster command located in the
     python virtual environment and the absolute path to the config
     ini file.

   * user: The unix user you are running CKAN with

   * stdout_logfile and stderr_logfile: All output coming from the
     harvest consumers will be written to this file. Ensure that the
     necessary permissions are setup.

   The rest of the configuration options are pretty self explanatory. Refer
   to the `Supervisor documentation <http://supervisord.org/configuration.html#program-x-section-settings>`_
   to know more about these and other options available.

3. Start the supervisor tasks with the following commands::

    sudo supervisorctl reread
    sudo supervisorctl add ckan_gather_consumer
    sudo supervisorctl add ckan_fetch_consumer
    sudo supervisorctl start ckan_gather_consumer
    sudo supervisorctl start ckan_fetch_consumer

   To check that the processes are running, you can run::

    sudo supervisorctl status

    ckan_fetch_consumer              RUNNING    pid 6983, uptime 0:22:06
    ckan_gather_consumer             RUNNING    pid 6968, uptime 0:22:45

   Some problems you may encounter when starting the processes:

   * `ckan_gather_consumer: ERROR (no such process)`
      Double-check your supervisor configuration file and stop and restart the supervisor daemon::

           sudo service supervisor start; sudo service supervisor stop

   * `ckan_gather_consumer: ERROR (abnormal termination)`
      Something prevented the command from running properly. Have a look at the log file that
      you defined in the `stdout_logfile` section to see what happened. Common errors include::

          `socket.error: [Errno 111] Connection refused`
          RabbitMQ is not running::

            sudo service rabbitmq-server start

4. Once we have the two consumers running and monitored, we just need to create a cron job
   that will run the `run` harvester command periodically. To do so, edit the cron table with
   the following command (it may ask you to choose an editor)::

    sudo crontab -e -u ckan

   Note that we are running this command as the same user we configured the
   processes to be run with (`ckan` in our example).

   Paste this line into your crontab, again replacing the paths to paster and
   the ini file with yours:

   ON CKAN >= 2.9::

    # m  h  dom mon dow   command
    */15 *  *   *   *     /usr/lib/ckan/default/bin/ckan -c /etc/ckan/default/ckan.ini harvester run

   ON CKAN <= 2.8::

    # m  h  dom mon dow   command
    */15 *  *   *   *     /usr/lib/ckan/default/bin/paster --plugin=ckanext-harvest harvester run --config=/etc/ckan/default/production.ini

   This particular example will check for pending jobs every fifteen minutes.
   You can of course modify this periodicity, this `Wikipedia page <http://en.wikipedia.org/wiki/Cron#CRON_expression>`_
   has a good overview of the crontab syntax.

5. In order to setup clean-up mechanism for the harvest log one more cron job needs to be scheduled::

    sudo crontab -e -u ckan

   Paste this line into your crontab, again replacing the paths to paster/ckan and
   the ini file with yours:

   ON CKAN >= 2.9::

    # m  h  dom mon dow   command
      0  5  *   *   *     /usr/lib/ckan/default/bin/ckan -c /etc/ckan/default/ckan.ini harvester clean_harvest_log

   ON CKAN <= 2.8::

    # m  h  dom mon dow   command
      0  5  *   *   *     /usr/lib/ckan/default/bin/paster --plugin=ckanext-harvest harvester clean_harvest_log --config=/etc/ckan/default/production.ini

   This particular example will perform clean-up each day at 05 AM.
   You can tweak the value according to your needs.

Extensible actions
==================

Recipients on harvest jobs notifications
----------------------------------------

:code:`harvest_get_notifications_recipients`: you can *chain* this action from another extension to change 
the recipients for harvest jobs notifications.

.. code-block:: python

  @toolkit.chained_action
  def harvest_get_notifications_recipients(up_func, context, data_dict):
      """ Harvester plugin notify by default about harvest jobs only to 
              admin users of the related organization.
              Also allow to add custom recipients with this function.
              
          Return a list of dicts with name and email like
              {'name': 'John', 'email': 'john@source.com'} """

      recipients = up_func(context, data_dict)
      new_recipients = []

      # you custom logic to add new_recipients here
      # new_recipients.append({'name': 'Harvester Admin', 'email': 'admin@harvester-team.com'})
      # recipients += new_recipients
      return recipients


Tests
=====

You can run the tests like this::

    cd ckanext-harvest
    nosetests --reset-db --ckan --with-pylons=test-core.ini ckanext/harvest/tests

Here are some common errors and solutions:

* ``(OperationalError) no such table: harvest_object_error u'delete from "harvest_object_error"``
  The database has got into in a bad state. Run the tests again but with the ``--reset-db`` parameter.

* ``(ProgrammingError) relation "harvest_object_extra" does not exist``
  The database has got into in a bad state. Run the tests again but *without* the ``--reset-db`` parameter.
  Alternatively it's because you forgot to use the ``--ckan`` parameter.

* ``(OperationalError) near "SET": syntax error``
  You are testing with SQLite as the database, but the CKAN Harvester needs PostgreSQL. Specify test-core.ini instead of test.ini.


Harvest API
=====

ckanext-harvest has multiple API's exposed in the format `/api/action/<endpoint>`.

* `/api/action/harvest_source_list`

This endpoint will return all the harvest sources in CKAN with a default limit
of 100 items. The limit can be set to a bespoke value in the config for ckan
under `ckan.harvest.harvest_source_limit`.

An optional query param `organization_id` can be used to narrow down the
results to only return the harvest sources created by certain organization's by
supplying their respective organization id -> `/api/action/harvest_source_list?organization_id=<some-org-id>`


Releases
========

To create a new release, follow the following steps:

* Determine new release number based on the rules of `semantic versioning <http://semver.org>`_
* Update the CHANGELOG, especially the link for the "Unreleased" section
* Update the version number in `setup.py`
* Create a new release on GitHub and add the CHANGELOG of this release as release notes


Community
=========

* Developer mailing list: `ckan-dev@lists.okfn.org <http://lists.okfn.org/mailman/listinfo/ckan-dev>`_
* Developer IRC channel: `#ckan on irc.freenode.net <http://webchat.freenode.net/?channels=ckan>`_
* `Issue tracker <https://github.com/ckan/ckanext-harvest/issues>`_


Contributing
============

For contributing to ckanext-harvest or its documentation, follow the guidelines described in
`CONTRIBUTING <https://github.com/ckan/ckanext-harvest/blob/master/CONTRIBUTING.rst>`_.


License
=======

This extension is open and licensed under the GNU Affero General Public License (AGPL) v3.0.
Its full text may be found at:

http://www.fsf.org/licensing/licenses/agpl-3.0.html


.. _Supervisor: http://supervisord.org

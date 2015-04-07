=============================================
ckanext-harvest - Remote harvesting extension
=============================================

.. image:: https://travis-ci.org/ckan/ckanext-harvest.svg?branch=master
    :target: https://travis-ci.org/ckan/ckanext-harvest

This extension provides a common harvesting framework for ckan extensions
and adds a CLI and a WUI to CKAN to manage harvesting sources and jobs.

Installation
============

1. The harvest extension can use two different backends. You can choose whichever
   you prefer depending on your needs, but Redis has been found to be more stable
   and reliable so it is the recommended one:

   * `Redis <http://redis.io/>`_ (recommended): To install it, run::

      sudo apt-get install redis-server

     On your CKAN configuration file, add::

      ckan.harvest.mq.type = redis

   * `RabbitMQ <http://www.rabbitmq.com/>`_: To install it, run::

      sudo apt-get install rabbitmq-server

     On your CKAN configuration file, add::

      ckan.harvest.mq.type = rabbitmq


2. Install the extension into your python environment.

   *Note:* Depending on the CKAN core version you are targeting you will need to
   use a different branch from the extension.

   For a production site, use the `stable` branch, unless there is a specific
   branch that targets the CKAN core version that you are using.

   To target the latest CKAN core release::

     (pyenv) $ pip install -e git+https://github.com/okfn/ckanext-harvest.git@stable#egg=ckanext-harvest

   To target an old release (if a release branch exists, otherwise use `stable`)::

     (pyenv) $ pip install -e git+https://github.com/okfn/ckanext-harvest.git@release-v1.8#egg=ckanext-harvest

   To target CKAN `master`, use the extension `master` branch (ie no branch defined)::

     (pyenv) $ pip install -e git+https://github.com/okfn/ckanext-harvest.git#egg=ckanext-harvest

3. Install the rest of python modules required by the extension::

     (pyenv) $ pip install -r pip-requirements.txt

4. Make sure the CKAN configuration ini file contains the harvest main plugin, as
   well as the harvester for CKAN instances if you need it (included with the extension)::

    ckan.plugins = harvest ckan_harvester

5. If you haven't done it yet on the previous step, define the backend that you are using with the ``ckan.harvest.mq.type``
   option (it defaults to ``rabbitmq``)::

    ckan.harvest.mq.type = redis


Configuration
=============

Run the following command to create the necessary tables in the database::

    paster --plugin=ckanext-harvest harvester initdb --config=mysite.ini

Finally, restart CKAN to have the changes take affect:

    sudo service apache2 restart

After installation, the harvest source listing should be available under /harvest, eg:

    http://localhost:5000/harvest


Command line interface
======================

The following operations can be run from the command line using the
``paster --plugin=ckanext-harvest harvester`` command::

      harvester initdb
        - Creates the necessary tables in the database

      harvester source {name} {url} {type} [{title}] [{active}] [{owner_org}] [{frequency}] [{config}]
        - create new harvest source

      harvester rmsource {id}
        - remove (inactivate) a harvester source

      harvester sources [all]
        - lists harvest sources
          If 'all' is defined, it also shows the Inactive sources

      harvester job {source-id}
        - create new harvest job

      harvester jobs
        - lists harvest jobs

      harvester run
        - runs harvest jobs

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester purge_queues
        - removes all jobs from fetch and gather queue

      harvester [-j] [--segments={segments}] import [{source-id}]
        - perform the import stage with the last fetched objects, optionally belonging to a certain source.
          Please note that no objects will be fetched from the remote server. It will only affect
          the last fetched objects already present in the database.

          If the -j flag is provided, the objects are not joined to existing datasets. This may be useful
          when importing objects for the first time.

          The --segments flag allows to define a string containing hex digits that represent which of
          the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f

      harvester job-all
        - create new harvest jobs for all active sources.

      harvester reindex
        - reindexes the harvest source datasets

The commands should be run with the pyenv activated and refer to your sites configuration file (mysite.ini in this example)::

        paster --plugin=ckanext-harvest harvester sources --config=mysite.ini

Authorization
=============

Starting from CKAN 2.0, harvest sources behave exactly the same as datasets
(they are actually internally implemented as a dataset type). That means that
can be searched and faceted, and that the same authorization rules can be
applied to them. The default authorization settings are based on organizations
(equivalent to the `publisher profile` found in old versions).

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
    Tags don't need to previously exist.

*   default_groups: A list of groups to which the harvested datasets will be
    added to. The groups must exist. Note that you must use ids or names to
    define the groups according to the API version you defined (names for version
    1, ids for version 2).

*   default_extras: A dictionary of key value pairs that will be added to extras
    of the harvested datasets. You can use the following replacement strings,
    that will be replaced before creating or updating the datasets:

    * {dataset_id}
    * {harvest_source_id}
    * {harvest_source_url}   # Will be stripped of trailing forward slashes (/)
    * {harvest_source_title}   # Requires CKAN 1.6
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

Here is an example of a configuration object (the one that must be entered in
the configuration field)::

    {
     "api_version": 1,
     "default_tags":["new-tag-1","new-tag-2"],
     "default_groups":["my-own-group"],
     "default_extras":{"new_extra":"Test","harvest_url":"{harvest_source_url}/dataset/{dataset_id}"},
     "override_extras": true,
     "user":"harverster-user",
     "api_key":"<REMOTE_API_KEY>",
     "read_only": true,
     "remote_groups": "only_local",
     "remote_orgs": "create"
    }


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
        Harvesting implementations must provide this method, which will return a
        dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This will
          appear on the form as a guidance to the user.

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

        Harvesters can provide this method to validate the configuration entered in the
        form. It should return a single string, which will be stored in the database.
        Exceptions raised will be shown in the form's error messages.

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
        The gather stage will recieve a HarvestJob object and will be
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
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''


See the CKAN harvester for an example of how to implement the harvesting
interface:

 ckanext-harvest/ckanext/harvest/harvesters/ckanharvester.py

Here you can also find other examples of custom harvesters:

* https://github.com/okfn/ckanext-pdeu/tree/master/ckanext/pdeu/harvesters
* https://github.com/okfn/ckanext-inspire/ckanext/inspire/harvesters.py


Running the harvest jobs
========================

The harvesting extension uses two different queues, one that handles the
gathering and another one that handles the fetching and importing. To start
the consumers run the following command
(make sure you have your python environment activated)::

      paster --plugin=ckanext-harvest harvester gather_consumer --config=mysite.ini

On another terminal, run the following command::

      paster --plugin=ckanext-harvest harvester fetch_consumer --config=mysite.ini

Finally, on a third console, run the following command to start any
pending harvesting jobs::

      paster --plugin=ckanext-harvest harvester run --config=mysite.ini

The ``run`` command not only starts any pending harvesting jobs, but also
flags those that are finished, allowing new jobs to be created on that particular
source and refreshing the source statistics. That means that you will need to run
this command before being able to create a new job on a source that was being
harvested (On a production site you will tipically have a cron job that runs the
command regularly, see next section).


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
(generally the one you are running CKAN with). Replace the user `okfn` in the
following steps with the one you are using.

1. Install Supervisor::

       sudo apt-get install supervisor

   You can check if it is running with this command::

       ps aux | grep supervisord

   You should see a line similar to this one::

       root      9224  0.0  0.3  56420 12204 ?        Ss   15:52   0:00 /usr/bin/python /usr/bin/supervisord

2. Supervisor needs to have programs added to its configuration, which will
   describe the tasks that need to be monitored. This configuration files are
   stored in ``/etc/supervisor/conf.d``.

   Create a file named ``/etc/supervisor/conf.d/ckan_harvesting.conf``, and copy the following contents::


        ; ===============================
        ; ckan harvester
        ; ===============================

        [program:ckan_gather_consumer]

        command=/var/lib/ckan/std/pyenv/bin/paster --plugin=ckanext-harvest harvester gather_consumer --config=/etc/ckan/std/std.ini

        ; user that owns virtual environment.
        user=okfn

        numprocs=1
        stdout_logfile=/var/log/ckan/std/gather_consumer.log
        stderr_logfile=/var/log/ckan/std/gather_consumer.log
        autostart=true
        autorestart=true
        startsecs=10

        [program:ckan_fetch_consumer]

        command=/var/lib/ckan/std/pyenv/bin/paster --plugin=ckanext-harvest harvester fetch_consumer --config=/etc/ckan/std/std.ini

        ; user that owns virtual environment.
        user=okfn

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

    sudo crontab -e -u okfn

   Note that we are running this command as the same user we configured the processes to be run with
   (`okfn` in our example).

   Paste this line into your crontab, again replacing the paths to paster and the ini file with yours::

    # m  h  dom mon dow   command
    */15 *  *   *   *     /var/lib/ckan/std/pyenv/bin/paster --plugin=ckanext-harvest harvester run --config=/etc/ckan/std/std.ini

   This particular example will check for pending jobs every fifteen minutes.
   You can of course modify this periodicity, this `Wikipedia page <http://en.wikipedia.org/wiki/Cron#CRON_expression>`_
   has a good overview of the crontab syntax.


.. _Supervisor: http://supervisord.org


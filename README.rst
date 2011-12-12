=============================================
ckanext-harvest - Remote harvesting extension
=============================================

This extension provides a common harvesting framework for ckan extensions
and adds a CLI and a WUI to CKAN to manage harvesting sources and jobs.

Installation
============

The harvest extension uses Message Queuing to handle the different gather
stages.

You will need to install the RabbitMQ server::

    sudo apt-get install rabbitmq-server

Clone the repository and set up the extension::

    git clone https://github.com/okfn/ckanext-harvest
    cd ckanext-harvest
    pip install -r pip-requirements.txt
    python setup.py develop

Make sure the CKAN configuration ini file contains the harvest main plugin, as
well as the harvester for CKAN instances (included with the extension)::

    ckan.plugins = harvest ckan_harvester


Configuration
=============

Run the following command (in the ckanext-harvest directory) to create
the necessary tables in the database::

    paster harvester initdb --config=../ckan/development.ini

The extension needs a user with sysadmin privileges to perform the
harvesting jobs. You can create such a user running these two commands in
the ckan directory::

    paster user add harvest

    paster sysadmin add harvest

After installation, the harvest interface should be available under /harvest
if you're logged in with sysadmin permissions, eg.

	http://localhost:5000/harvest


Command line interface
======================

The following operations can be run from the command line using the
``paster harvester`` command::

      harvester initdb
        - Creates the necessary tables in the database

      harvester source {url} {type} [{active}] [{user-id}] [{publisher-id}]
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

      harvester import [{source-id}]
        - perform the import stage with the last fetched objects, optionally
          belonging to a certain source.
          Please note that no objects will be fetched from the remote server.
          It will only affect the last fetched objects already present in the
          database.

      harvester job-all
        - create new harvest jobs for all active sources.

The commands should be run from the ckanext-harvest directory and expect
a development.ini file to be present. Most of the time you will specify
the config explicitly though::

        paster harvester sources --config=../ckan/development.ini

The CKAN haverster
==================

The plugin includes a harvester for remote CKAN instances. To use it, you need
to add the `ckan_harvester` plugin to your options file::

	ckan.plugins = harvest ckan_harvester

After adding it, a 'CKAN' option should appear in the 'New harvest source' form.


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
        * form_config_interface [optional]: Harvesters willing to store configuration
          values in the database must provide this key. The only supported value is
          'Text'. This will enable the configuration text box in the form. See also
          the ``validate_config`` method.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        returns: A dictionary with the harvester descriptors
        '''

    def validate_config(self, config):
        '''
        Harvesters can provide this method to validate the configuration entered in the
        form. It should return a single string, which will be stored in the database.
        Exceptions raised will be shown in the form's error messages.

        returns A string with the validated configuration options
        '''

    def gather_stage(self, harvest_job):
        '''
        The gather stage will recieve a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its source and job.
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

See the CKAN harvester for a an example on how to implement the harvesting
interface:

    ckanext-harvest/ckanext/harvest/harvesters/ckanharvester.py

Here you can also find other examples of custom harvesters:

    https://github.com/okfn/ckanext-pdeu/tree/master/ckanext/pdeu/harvesters


Running the harvest jobs
========================

The harvesting extension uses two different queues, one that handles the
gathering and another one that handles the fetching and importing. To start
the consumers run the following command from the ckanext-harvest directory
(make sure you have your python environment activated)::

      paster harvester gather_consumer --config=../ckan/development.ini

On another terminal, run the following command::

      paster harvester fetch_consumer --config=../ckan/development.ini

Finally, on a third console, run the following command to start any
pending harvesting jobs::

      paster harvester run --config=../ckan/development.ini
      
After packages have been imported, the search index will have to be updated
before the packages appear in search results (from the ckan directory):

      paster search-index

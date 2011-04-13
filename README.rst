================================================
ckanext-harvest - Remote harvesting extension
================================================

This extension will contain all harvesting related code, now present
in ckan core, ckanext-dgu and ckanext-csw.

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

The user's API key must be defined in the CKAN
configuration file (.ini) in the [app:main] section::

    ckan.harvesting.api_key = 4e1dac58-f642-4e54-bbc4-3ea262271fe2


The API URL used can be also defined in the ini file (it defaults to 
http://localhost:5000/)::

    ckan.api_url = <api_url>

Tests
=====

To run the tests, this is the basic command::

    $ nosetests --ckan tests/

Or with postgres::

    $ nosetests --ckan --with-pylons=../ckan/test-core.ini tests/

(See the Ckan README for more information.)


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
       
The commands should be run from the ckanext-harvest directory and expect
a development.ini file to be present. Most of the time you will specify 
the config explicitly though::

        paster harvester sources --config=../ckan/development.ini


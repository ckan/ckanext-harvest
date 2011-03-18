================================================
ckanext-harvest - Remote harvesting extension
================================================

This extension will contain all harvesting related code, now present
in ckan core, ckanext-dgu and ckanext-csw.

Dependencies
============

You will need ckan installed, as well as the ckanext-dgu and ckanext-csw
plugins activated.


Configuration
=============

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
 

Command line interface
======================

The following operations can be run from the command line using the 
``paster harvester`` command::

      harvester source {url} [{user-ref} [{publisher-ref}]]     
        - create new harvest source

      harvester rmsource {url}
        - remove a harvester source (and associated jobs)

      harvester sources                                 
        - lists harvest sources

      harvester job {source-id} [{user-ref}]
        - create new harvesting job

      harvester rmjob {job-id}
        - remove a harvesting job
  
      harvester jobs
        - lists harvesting jobs

      harvester run
        - runs harvesting jobs

      harvester extents
        - creates or updates the extent geometry column for packages with
          a bounding box defined in extras
       
The commands should be run from the ckanext-harvest directory and expect
a development.ini file to be present. Most of the time you will specify 
the config explicitly though::

        paster harvester sources --config=../ckan/development.ini


API
===

The extension adds the following call to the CKAN search API, which returns
packages with an extent that intersects with the bounding box provided::

    /api/2/search/package/geo?bbox={minx,miny,maxx,maxy}[&crs={srid}]

If the bounding box coordinates are not in the same projection as the one
defined in the database, a CRS must be provided, in one of the following
forms:

- urn:ogc:def:crs:EPSG::4258
- EPSG:4258
- 4258




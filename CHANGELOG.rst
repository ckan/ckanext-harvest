#########
Changelog
#########

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <http://keepachangelog.com>`_
and this project adheres to `Semantic Versioning <http://semver.org/>`_

***********
Unreleased_
***********

Changed
-------

- Migrate tests from Travis CI to GitHub Actions


***********
1.3.2_ - 2020-10-08
***********

Changed
-------

- Calculate timeouts based on last finished object instead of job creation time #418

Fixed
-----

- Fix resubmitting harvest objects to Redis fetch queue #421


***********
1.3.1_ - 2020-09-01
***********

Changed
-------

- Abort failed jobs CLI command #398

Fixed
-----

- Fix Redis conflict with core workers
- Fix harvest source list reference
- Fix and improve test suite, remove nose tests


***********
1.3.0_ - 2020-06-04
***********

Changed
-------

- Support for Python 3 #392
- Add option for job timeout #403
- Add support for limiting number of results and filtering by organization in harvest_source_list #403

Fixed
-----

- Fix support for different Redis client libraries #403
- Fix force_import option in run_test command #402
- Fix show object #395
- Fix handling of exceptions in controller #390


***********
1.2.1_ - 2020-01-22
***********

Changed
-------

- Support ``not modified`` status for objects #385
- New ``force-import`` flag for the ``run_test`` command #385

Fixed
-----

- Get message from harvest_object_error-dict #381
- Fix Admin link appearing to non authorized users #389
- Capture Redis Exceptions #385

*******************
1.2.0_ - 2019-11-01
*******************

Changed
-------
- Apply flake8 to be PEP-8 compliant #354
- Use ckantoolkit to clean up imports #358
- Add hook to extend the package dict in CKAN harvester
- Use CKAN core ckan.redis.url setting if present
- Remove database migration code targeting ancient versions #376
    (In the unlikely event that you need to upgrade from one
     of the previous DB versions just apply the changes removed
     on the linked PR manually)

Fixed
-----
- harvest_source_type_exists validator should not fail if Harvester has no ``info()`` method #338
- Fix SSL problems for old versions of Python 2.7.x #344
- Add an 'owner_org' to the v3 package migration #348
- Fix harvest request exceptions #357
- Fix wrong toolkit reference 8e862c8
- Mark early errored jobs as finished 5ad6d86
- Resubmit awaiting objects in the DB not on Redis 5ffe6d4

*******************
1.1.4_ - 2018-10-26
*******************
Fixed
-----
- Fix nav link

*******************
1.1.3_ - 2018-10-26
*******************
Fixed
-----
- Reduce usage of c vars (CKAN 2.9)

*******************
1.1.2_ - 2018-10-25
*******************
Added
-----
- Send harvest-error-mails to organization-admins #329
- CKAN Harvester option to include/exclude groups #323
- Use Redis password from configuration when present #332
- Support for CKAN 2.9

Fixed
-----
- Ensures the AND operator for fq in solr #335
- Fix styling issues on Bootstrap 3

*******************
1.1.1_ - 2018-06-13
*******************
Added
-----
- Move CKANHarvester._last_error_free_job to HarvesterBase.last_error_free_job #305
- Add the CSS classes for FontAwesome 4.x #313
- Add config option for dataset name append type #327
- Send error mail to admin when harvesting fails #244

Changed
-------
- Readme test tip ckan parameter #318

Fixed
-----
- Fix handling of ``clean_tags`` options for tag lists and dicts #304
- Don't delete all solr documents/fail to index harvesters when harvest config blank #315
- Fix print statements to be Py3 friendly #328

*******************
1.1.0_ - 2017-11-07
*******************
Added
-----
- Button on harvest admin page to abort running jobs #296

Changed
-------
- Test improvements for harvester config #288
- Use package_search API for count of datasets #298
- Catch sqlalchemy.exc.DatabaseError instead of sqlalchemy.exc.OperationalError in ``gather_callback`` #301

Fixed
-------
- Fix default_extras initialization #290
- Travis build (postgres service, checkout of correct CKAN branch, libcommons-fileupload) #297

*******************
1.0.0_ - 2017-03-30
*******************
Added
-----
- Includes i18n directory in package.
- Adds a new ``clearsource_history`` command/operation.
- Adds new parameter ``return_last_job_status`` to ``harvest_source_list``
- Documentation for logs API

Changed
-------
- ``gather_stage`` return empty list instead of None if errors occured
- Change ``redirect`` calls to ``h.redirect_to``

Fixed
-----
- Fix namespace package declarations
- Only purge own data when calling ``queue_purge`` with redis
- Fix ``default_groups`` behavior

*******************
0.0.5_ - 2016-05-23
*******************
Added
-----
- Adds ``HarvestLog`` to log to database
- Adds a new ``clean_harvest_log`` command to clean the log table

Removed
-------
- This release removes support for CKAN <= 2.0

*******************
0.0.4_ - 2015-12-11
*******************
Added
-----
- Adds ``_find_existing_package`` method to allow harvesters extending the ``HarvesterBase`` to implement their own logic to find an existing package
- Adds support for ``ITranslation`` interface
- Adds special CSS class to datetimes in frontend to enable localisation to the users timezone

Changed
-------
- Make statistics keys consistent across all actions

Removed
-------
- Remove ``harvest_source_for_a_dataset`` action

*******************
0.0.3_ - 2015-11-20
*******************
Fixed
-----
- Fixed queues tests


*******************
0.0.2_ - 2015-11-20
*******************
Changed
-------
- Namespace redis keys to avoid conflicts between CKAN instances


*******************
0.0.1_ - 2015-11-20
*******************
Added
-----
- Adds clear source as a command
- Adds specific exceptions instead of having only the generic ``Exception``

Fixed
-----
- Catch 'no harvest job' exception

**********
Categories
**********
- ``Added`` for new features.
- ``Changed`` for changes in existing functionality.
- ``Deprecated`` for once-stable features removed in upcoming releases.
- ``Removed`` for deprecated features removed in this release.
- ``Fixed`` for any bug fixes.
- ``Security`` to invite users to upgrade in case of vulnerabilities.

.. _Unreleased: https://github.com/ckan/ckanext-harvest/compare/v1.3.2...HEAD
.. _1.3.2: https://github.com/ckan/ckanext-harvest/compare/v1.3.1...v1.3.2
.. _1.3.1: https://github.com/ckan/ckanext-harvest/compare/v1.3.0...v1.3.1
.. _1.3.0: https://github.com/ckan/ckanext-harvest/compare/v1.2.1...v1.3.0
.. _1.2.1: https://github.com/ckan/ckanext-harvest/compare/v1.2.0...v1.2.1
.. _1.2.0: https://github.com/ckan/ckanext-harvest/compare/v1.1.4...v1.2.0
.. _1.1.4: https://github.com/ckan/ckanext-harvest/compare/v1.1.3...v1.1.4
.. _1.1.3: https://github.com/ckan/ckanext-harvest/compare/v1.1.2...v1.1.3
.. _1.1.2: https://github.com/ckan/ckanext-harvest/compare/v1.1.1...v1.1.2
.. _1.1.1: https://github.com/ckan/ckanext-harvest/compare/v1.1.0...v1.1.1
.. _1.1.0: https://github.com/ckan/ckanext-harvest/compare/v1.0.0...v1.1.0
.. _1.0.0: https://github.com/ckan/ckanext-harvest/compare/v0.0.5...v1.0.0
.. _0.0.5: https://github.com/ckan/ckanext-harvest/compare/v0.0.4...v0.0.5
.. _0.0.4: https://github.com/ckan/ckanext-harvest/compare/v0.0.3...v0.0.4
.. _0.0.3: https://github.com/ckan/ckanext-harvest/compare/v0.0.2...v0.0.3
.. _0.0.2: https://github.com/ckan/ckanext-harvest/compare/v0.0.1...v0.0.2
.. _0.0.1: https://github.com/ckan/ckanext-harvest/compare/ckan-1.6...v0.0.1

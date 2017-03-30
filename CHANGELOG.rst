#########
Changelog
#########

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <http://keepachangelog.com>`_
and this project adheres to `Semantic Versioning <http://semver.org/>`_.

***********
Unreleased_
***********

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

.. _Unreleased: https://github.com/ckan/ckanext-harvest/compare/v1.0.0...HEAD
.. _1.0.0: https://github.com/ckan/ckanext-harvest/compare/v0.0.5...v1.0.0
.. _0.0.5: https://github.com/ckan/ckanext-harvest/compare/v0.0.4...v0.0.5
.. _0.0.4: https://github.com/ckan/ckanext-harvest/compare/v0.0.3...v0.0.4
.. _0.0.3: https://github.com/ckan/ckanext-harvest/compare/v0.0.2...v0.0.3
.. _0.0.2: https://github.com/ckan/ckanext-harvest/compare/v0.0.1...v0.0.2
.. _0.0.1: https://github.com/ckan/ckanext-harvest/compare/ckan-1.6...v0.0.1

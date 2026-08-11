# Changelog

## [Unreleased](https://github.com/ckan/ckanext-harvest/compare/v1.6.2...HEAD)

## [v1.6.2](https://github.com/ckan/ckanext-harvest/compare/v1.6.1...v1.6.2) - 2025-11-11

* Use Bootstrap 5 badge classes instead of old label classes ([#570](https://github.com/ckan/ckanext-harvest/pull/570))
* Drop temp table package_ids_to_delete ([#566](https://github.com/ckan/ckanext-harvest/pull/566))
* Pass search_facets into h.get_facet_items_dict ([#572](https://github.com/ckan/ckanext-harvest/pull/572))

## [v1.6.1](https://github.com/ckan/ckanext-harvest/compare/v1.6.0...v1.6.1) - 2025-01-14

* CKAN 2.9 is not longer maintained ([#559](https://github.com/ckan/ckanext-harvest/pull/559))
* Update manifest to include alembic configuration ([#558](https://github.com/ckan/ckanext-harvest/pull/558))

## [v1.6.0](https://github.com/ckan/ckanext-harvest/compare/v1.5.6...v1.6.0) - 2024-10-31

* CKAN 2.11 support ([#551](https://github.com/ckan/ckanext-harvest/pull/551))
* Switched to alembic migrations ([#540](https://github.com/ckan/ckanext-harvest/pull/540))
* Support for SQLAlchemy 2 ([#553](https://github.com/ckan/ckanext-harvest/pull/553))
* Use pyproject.toml file ([#554](https://github.com/ckan/ckanext-harvest/pull/554))
* Add tab for harvest sources in sysadmin page
* Clean up harvest source clear command, fix revisions exception ([#556](https://github.com/ckan/ckanext-harvest/pull/556))
* Convert boolean values to bools ([#544](https://github.com/ckan/ckanext-harvest/pull/544))

## [v1.5.6](https://github.com/ckan/ckanext-harvest/compare/v1.5.5...v1.5.6) - 2023-06-26

* Fix url endpoint for job_show ([#534](https://github.com/ckan/ckanext-harvest/pull/534))

## [v1.5.5](https://github.com/ckan/ckanext-harvest/compare/v1.5.4...v1.5.5) - 2023-06-05

* Fix display of harvest job errors ([#533](https://github.com/ckan/ckanext-harvest/pull/533))

## [v1.5.4](https://github.com/ckan/ckanext-harvest/compare/v1.5.3...v1.5.4) - 2023-05-23

* Fix a problem with data-dictization when using sqlalchemy 1.4+ ([#529](https://github.com/ckan/ckanext-harvest/pull/529))

## [v1.5.3](https://github.com/ckan/ckanext-harvest/compare/v1.5.2...v1.5.3) - 2023-04-03

* Fix asset path in MANIFEST.in ([#525](https://github.com/ckan/ckanext-harvest/pull/525))

## [v1.5.2](https://github.com/ckan/ckanext-harvest/compare/v1.5.1...v1.5.2) - 2023-03-28

* Fix URL endpoints: from `harvest.object_show` to `harvester.object_show` ([#524](https://github.com/ckan/ckanext-harvest/pull/524))

## [v1.5.1](https://github.com/ckan/ckanext-harvest/compare/v1.5.0...v1.5.1) - 2023-03-22

* Fix `url_for` routing to point to harvester blueprint ([#523](https://github.com/ckan/ckanext-harvest/pull/523))

## [v1.5.0](https://github.com/ckan/ckanext-harvest/compare/v1.4.2...v1.5.0) - 2023-03-16

* Added unescape for email text body to avoid encoded characters ([#517](https://github.com/ckan/ckanext-harvest/pull/517))
* Pick the right harvest_object_id if there are multiple ([#519](https://github.com/ckan/ckanext-harvest/pull/519))
* Do not duplicate harvest_extras if exist in root schema ([#521](https://github.com/ckan/ckanext-harvest/pull/521))
* Use 403 when actions are forbidden, not 401 ([#522](https://github.com/ckan/ckanext-harvest/pull/522))
* Drop support old versions ([#520](https://github.com/ckan/ckanext-harvest/pull/520))
* **Breaking change**: `h.bootstrap_version()` no longer exist since it is no longer needed to inject CSS classes
* **Breaking change**: Support for old Pylon's route syntax has been removed. Example: calling `url_for("harvest_read")` will no longer work. URLs for `ckanext-harvest` needs to respect Flask's syntax: `url_for("harvest.read")`, etc

## [v1.4.2](https://github.com/ckan/ckanext-harvest/compare/v1.4.1...v1.4.2) - 2023-01-12

* Add DB index harvest_error_harvest_object_id_idx ([#514](https://github.com/ckan/ckanext-harvest/pull/514))
* Remove pyopenssl requirement ([c87309a](https://github.com/ckan/ckanext-harvest/commit/c87309a))
* Add CSRF protection to new source form ([#516](https://github.com/ckan/ckanext-harvest/pull/516))

## [v1.4.1](https://github.com/ckan/ckanext-harvest/compare/v1.4.0...v1.4.1) - 2022-09-20

* Use requirements.txt instead of pip-requirements.txt (still working via symlink) ([8ed1eca](https://github.com/ckan/ckanext-harvest/commit/8ed1eca))
* Bump pyopenssl requirement to avoid requirements error on install ([98edcd3](https://github.com/ckan/ckanext-harvest/commit/98edcd3))
* Fixes unicode error in Python 2 ([#502](https://github.com/ckan/ckanext-harvest/pull/502))
* Fixes in email notification sendngi ([#499](https://github.com/ckan/ckanext-harvest/pull/499), [#505](https://github.com/ckan/ckanext-harvest/pull/505))
* Fix pagination for Dataset list on source page ([#504](https://github.com/ckan/ckanext-harvest/pull/504))

## [v1.4.0](https://github.com/ckan/ckanext-harvest/compare/v1.3.4...v1.4.0) - 2022-04-20

* Add ckan.harvest.not_overwrite_fields ([#472](https://github.com/ckan/ckanext-harvest/pull/472))
* Support for Bootstrap 5 templates ([#490](https://github.com/ckan/ckanext-harvest/pull/490))
* Support for CKAN 2.10 ([#492](https://github.com/ckan/ckanext-harvest/pull/492), [#496](https://github.com/ckan/ckanext-harvest/pull/496))
* Fix JSONDecode error ([#489](https://github.com/ckan/ckanext-harvest/pull/489))
* Check if email exists before sending notification ([#498](https://github.com/ckan/ckanext-harvest/pull/498))

## [v1.3.4](https://github.com/ckan/ckanext-harvest/compare/v1.3.3...v1.3.4) - 2022-01-24

* Changes function calls to `render_jinja2` over to `render` as the former is
  no longer used. ([#459](https://github.com/ckan/ckanext-harvest/pull/459))
* Set the default value for MQ_TYPE to redis ([#463](https://github.com/ckan/ckanext-harvest/pull/463))
* Add option `keep-current` to `clearsource_history` command ([#484](https://github.com/ckan/ckanext-harvest/pull/484))
* Fix JSON serialization for Python3 ([#450](https://github.com/ckan/ckanext-harvest/pull/450))
* Make `Rehavest` and `Clear` buttons work again ([#452](https://github.com/ckan/ckanext-harvest/pull/452))
* Fix error when running run-test ([#466](https://github.com/ckan/ckanext-harvest/pull/466))
* Fix timeout calculation ([#482](https://github.com/ckan/ckanext-harvest/pull/482))
* Fix harvest extras for packages ([#458](https://github.com/ckan/ckanext-harvest/pull/458))

## [v1.3.3](https://github.com/ckan/ckanext-harvest/compare/v1.3.2...v1.3.3) - 2021-03-26

* Migrate tests from Travis CI to GitHub Actions
* Optimize last error free job detection ([#437](https://github.com/ckan/ckanext-harvest/pull/437))
* Improve timeout detection ([#431](https://github.com/ckan/ckanext-harvest/pull/431))
* Check if Redis key is available ([#432](https://github.com/ckan/ckanext-harvest/pull/432))
* Include webassets.yml in MANIFEST

## [v1.3.2](https://github.com/ckan/ckanext-harvest/compare/v1.3.1...v1.3.2) - 2020-10-08

* Calculate timeouts based on last finished object instead of job creation time ([#418](https://github.com/ckan/ckanext-harvest/pull/418))
* Fix resubmitting harvest objects to Redis fetch queue ([#421](https://github.com/ckan/ckanext-harvest/pull/421))

## [v1.3.1](https://github.com/ckan/ckanext-harvest/compare/v1.3.0...v1.3.1) - 2020-09-01

* Abort failed jobs CLI command ([#398](https://github.com/ckan/ckanext-harvest/pull/398))
* Fix Redis conflict with core workers
* Fix harvest source list reference
* Fix and improve test suite, remove nose tests

## [v1.3.0](https://github.com/ckan/ckanext-harvest/compare/v1.2.1...v1.3.0) - 2020-06-04

* Support for Python 3 ([#392](https://github.com/ckan/ckanext-harvest/pull/392))
* Add option for job timeout ([#403](https://github.com/ckan/ckanext-harvest/pull/403))
* Add support for limiting number of results and filtering by organization in harvest_source_list ([#403](https://github.com/ckan/ckanext-harvest/pull/403))
* Fix support for different Redis client libraries ([#403](https://github.com/ckan/ckanext-harvest/pull/403))
* Fix force_import option in run_test command ([#402](https://github.com/ckan/ckanext-harvest/pull/402))
* Fix show object ([#395](https://github.com/ckan/ckanext-harvest/pull/395))
* Fix handling of exceptions in controller ([#390](https://github.com/ckan/ckanext-harvest/pull/390))

## [v1.2.1](https://github.com/ckan/ckanext-harvest/compare/v1.2.0...v1.2.1) - 2020-01-22

* Support `not modified` status for objects ([#385](https://github.com/ckan/ckanext-harvest/pull/385))
* New `force-import` flag for the `run_test` command ([#385](https://github.com/ckan/ckanext-harvest/pull/385))
* Get message from harvest_object_error-dict ([#381](https://github.com/ckan/ckanext-harvest/pull/381))
* Fix Admin link appearing to non authorized users ([#389](https://github.com/ckan/ckanext-harvest/pull/389))
* Capture Redis Exceptions ([#385](https://github.com/ckan/ckanext-harvest/pull/385))

## [v1.2.0](https://github.com/ckan/ckanext-harvest/compare/v1.1.4...v1.2.0) - 2019-11-01

* Apply flake8 to be PEP-8 compliant ([#354](https://github.com/ckan/ckanext-harvest/pull/354))
* Use ckantoolkit to clean up imports ([#358](https://github.com/ckan/ckanext-harvest/pull/358))
* Add hook to extend the package dict in CKAN harvester
* Use CKAN core ckan.redis.url setting if present
* Remove database migration code targeting ancient versions ([#376](https://github.com/ckan/ckanext-harvest/pull/376))
  (In the unlikely event that you need to upgrade from one of the previous DB
  versions just apply the changes removed on the linked PR manually)
* harvest_source_type_exists validator should not fail if Harvester has no `info()` method ([#338](https://github.com/ckan/ckanext-harvest/pull/338))
* Fix SSL problems for old versions of Python 2.7.x ([#344](https://github.com/ckan/ckanext-harvest/pull/344))
* Add an 'owner_org' to the v3 package migration ([#348](https://github.com/ckan/ckanext-harvest/pull/348))
* Fix harvest request exceptions ([#357](https://github.com/ckan/ckanext-harvest/pull/357))
* Fix wrong toolkit reference ([8e862c8](https://github.com/ckan/ckanext-harvest/commit/8e862c8))
* Mark early errored jobs as finished ([5ad6d86](https://github.com/ckan/ckanext-harvest/commit/5ad6d86))
* Resubmit awaiting objects in the DB not on Redis ([5ffe6d4](https://github.com/ckan/ckanext-harvest/commit/5ffe6d4))

## [v1.1.4](https://github.com/ckan/ckanext-harvest/compare/v1.1.3...v1.1.4) - 2018-10-26

* Fix nav link

## [v1.1.3](https://github.com/ckan/ckanext-harvest/compare/v1.1.2...v1.1.3) - 2018-10-26

* Reduce usage of c vars (CKAN 2.9)

## [v1.1.2](https://github.com/ckan/ckanext-harvest/compare/v1.1.1...v1.1.2) - 2018-10-25

* Send harvest-error-mails to organization-admins ([#329](https://github.com/ckan/ckanext-harvest/pull/329))
* CKAN Harvester option to include/exclude groups ([#323](https://github.com/ckan/ckanext-harvest/pull/323))
* Use Redis password from configuration when present ([#332](https://github.com/ckan/ckanext-harvest/pull/332))
* Support for CKAN 2.9
* Ensures the AND operator for fq in solr ([#335](https://github.com/ckan/ckanext-harvest/pull/335))
* Fix styling issues on Bootstrap 3

## [v1.1.1](https://github.com/ckan/ckanext-harvest/compare/v1.1.0...v1.1.1) - 2018-06-13

* Move CKANHarvester._last_error_free_job to HarvesterBase.last_error_free_job ([#305](https://github.com/ckan/ckanext-harvest/pull/305))
* Add the CSS classes for FontAwesome 4.x ([#313](https://github.com/ckan/ckanext-harvest/pull/313))
* Add config option for dataset name append type ([#327](https://github.com/ckan/ckanext-harvest/pull/327))
* Send error mail to admin when harvesting fails ([#244](https://github.com/ckan/ckanext-harvest/pull/244))
* Readme test tip ckan parameter ([#318](https://github.com/ckan/ckanext-harvest/pull/318))
* Fix handling of `clean_tags` options for tag lists and dicts ([#304](https://github.com/ckan/ckanext-harvest/pull/304))
* Don't delete all solr documents/fail to index harvesters when harvest config blank ([#315](https://github.com/ckan/ckanext-harvest/pull/315))
* Fix print statements to be Py3 friendly ([#328](https://github.com/ckan/ckanext-harvest/pull/328))

## [v1.1.0](https://github.com/ckan/ckanext-harvest/compare/v1.0.0...v1.1.0) - 2017-11-07

* Button on harvest admin page to abort running jobs ([#296](https://github.com/ckan/ckanext-harvest/pull/296))
* Test improvements for harvester config ([#288](https://github.com/ckan/ckanext-harvest/pull/288))
* Use package_search API for count of datasets ([#298](https://github.com/ckan/ckanext-harvest/pull/298))
* Catch sqlalchemy.exc.DatabaseError instead of sqlalchemy.exc.OperationalError in `gather_callback` ([#301](https://github.com/ckan/ckanext-harvest/pull/301))
* Fix default_extras initialization ([#290](https://github.com/ckan/ckanext-harvest/pull/290))
* Travis build (postgres service, checkout of correct CKAN branch, libcommons-fileupload) ([#297](https://github.com/ckan/ckanext-harvest/pull/297))

## [v1.0.0](https://github.com/ckan/ckanext-harvest/compare/v0.0.5...v1.0.0) - 2017-03-30

* Includes i18n directory in package.
* Adds a new `clearsource_history` command/operation.
* Adds new parameter `return_last_job_status` to `harvest_source_list`
* Documentation for logs API
* `gather_stage` return empty list instead of None if errors occured
* Change `redirect` calls to `h.redirect_to`
* Fix namespace package declarations
* Only purge own data when calling `queue_purge` with redis
* Fix `default_groups` behavior

## [v0.0.5](https://github.com/ckan/ckanext-harvest/compare/v0.0.4...v0.0.5) - 2016-05-23

* Adds `HarvestLog` to log to database
* Adds a new `clean_harvest_log` command to clean the log table
* This release removes support for CKAN <= 2.0

## [v0.0.4](https://github.com/ckan/ckanext-harvest/compare/v0.0.3...v0.0.4) - 2015-12-11

* Adds `_find_existing_package` method to allow harvesters extending the `HarvesterBase` to implement their own logic to find an existing package
* Adds support for `ITranslation` interface
* Adds special CSS class to datetimes in frontend to enable localisation to the users timezone
* Make statistics keys consistent across all actions
* Remove `harvest_source_for_a_dataset` action

## [v0.0.3](https://github.com/ckan/ckanext-harvest/compare/v0.0.2...v0.0.3) - 2015-11-20

* Fixed queues tests

## [v0.0.2](https://github.com/ckan/ckanext-harvest/compare/v0.0.1...v0.0.2) - 2015-11-20

* Namespace redis keys to avoid conflicts between CKAN instances

## [v0.0.1](https://github.com/ckan/ckanext-harvest/compare/ckan-1.6...v0.0.1) - 2015-11-20

* Adds clear source as a command
* Adds specific exceptions instead of having only the generic `Exception`
* Catch 'no harvest job' exception

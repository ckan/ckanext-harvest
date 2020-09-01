# -*- coding: utf-8 -*-

from __future__ import print_function

import json
import logging
import re
import sys
import xml.etree.ElementTree as etree

import ckan.lib.helpers as h
import ckan.plugins.toolkit as tk
import six
from ckan import model
from ckantoolkit import _
from six import StringIO

from ckanext.harvest.logic import HarvestJobExists, HarvestSourceInactiveError

try:
    # Python 2.7
    xml_parser_exception = etree.ParseError
except AttributeError:
    # Python 2.6
    from xml.parsers import expat

    xml_parser_exception = expat.ExpatError

log = logging.getLogger(__name__)


DATASET_TYPE_NAME = "harvest"

###############################################################################
#                                     CLI                                     #
###############################################################################


def _admin_user():
    context = {"model": model, "session": model.Session, "ignore_auth": True}
    return tk.get_action("get_site_user")(context, {})


def _print_harvest_sources(sources, output):
    for source in sources:
        _print_harvest_source(source, output)


def _print_harvest_jobs(jobs, output):
    for job in jobs:
        _print_harvest_job(job, output)


def _print_harvest_job(job, output):
    print(
        ("\tJob id: {}\n"
         "\t\tstatus: {}\n"
         "\t\tsource: {}\n"
         "\t\tobjects: {}").format(
             job.get("id"),
             job.get("status"),
             job.get("source_id"),
             len(job.get("objects", [])),
         ),
        file=output,
    )

    print(
        "\t\tgather_errors: {0}".format(len(job.get("gather_errors", []))),
        file=output,
    )
    for error in job.get("gather_errors", []):
        print("\t\t\t{0}".format(error["message"]), file=output)


def _print_harvest_source(source, output):
    print("Source id: {0}".format(source.get("id")), file=output)
    if "name" in source:
        # 'name' is only there if the source comes from the Package
        print("\tname: {}".format(source["name"]), file=output)

    data_dict = {
        "url": source.get("url"),
        # 'type' if source comes from HarvestSource, 'source_type' if
        # it comes from the Package
        "type": source.get("source_type") or source.get("type"),
        "active": source.get("active",
                             source.get("state") == "active"),
        "frequency": source.get("frequency"),
        "jobs": source.get("status").get("job_count"),
    }
    print(
        ("\turl: {url}\n"
         "\ttype: {type}\n"
         "\tactive: {active}\n"
         "\tfrequency: {frequency}\n"
         "\tjobs: {jobs}\n"
         "\n").format(**data_dict),
        file=output,
    )


def _there_are(what, sequence, condition=""):
    is_singular = len(sequence) == 1
    return "There {0} {1} {2}{3}{4}".format(
        is_singular and "is" or "are",
        len(sequence),
        condition and ("{0} ".format(condition.lower())) or "",
        what,
        not is_singular and "s" or "",
    )


def initdb():
    from ckanext.harvest.model import setup as db_setup

    db_setup()


def create_harvest_source(
    name,
    url,
    type,
    title=None,
    active=True,
    owner_org=None,
    frequency="MANUAL",
    config=None,
):
    output = StringIO()

    data_dict = {
        "name": name,
        "url": url,
        "source_type": type,
        "title": title,
        "active": active,
        "owner_org": owner_org,
        "frequency": frequency,
        "config": config,
    }

    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
        "ignore_auth": True,
    }
    source = tk.get_action("harvest_source_create")(context, data_dict)

    print("Created new harvest source:", file=output)
    _print_harvest_source(source, output)

    sources = tk.get_action("harvest_source_list")(context, {})
    print(_there_are("harvest source", sources), file=output)

    # Create a harvest job for the new source if not regular job.
    if not data_dict["frequency"]:
        tk.get_action("harvest_job_create")(context, {
            "source_id": source["id"],
            "run": True
        })
        print(
            "A new Harvest Job for this source has also been created",
            file=output,
        )

    return output.getvalue()


def show_harvest_source(source_id_or_name):
    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    source = tk.get_action("harvest_source_show")(context, {
        "id": source_id_or_name
    })
    output = StringIO()
    _print_harvest_source(source, output)
    return output.getvalue()


def remove_harvest_source(source_id_or_name):
    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    source = tk.get_action("harvest_source_show")(context, {
        "id": source_id_or_name
    })
    tk.get_action("harvest_source_delete")(context, {"id": source["id"]})


def clear_harvest_source(source_id_or_name):
    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    source = tk.get_action("harvest_source_show")(context, {
        "id": source_id_or_name
    })
    tk.get_action("harvest_source_clear")(context, {"id": source["id"]})


def clear_harvest_source_history(source_id):

    context = {
        "model": model,
        "user": _admin_user()["name"],
        "session": model.Session,
    }
    if source_id is not None:
        tk.get_action("harvest_source_job_history_clear")(context, {
            "id": source_id
        })
        return "Cleared job history of harvest source: {0}".format(source_id)
    else:
        # Purge queues, because we clean all harvest jobs and
        # objects in the database.
        purge_queues()
        cleared_sources_dicts = tk.get_action(
            "harvest_sources_job_history_clear")(context, {})
        return "Cleared job history for all harvest sources: {0} source(s)".format(
            len(cleared_sources_dicts))


def abort_failed_jobs(job_life_span, include, exclude):
    context = {
        "model": model,
        "user": _admin_user()["name"],
        "session": model.Session,
    }
    result = tk.get_action("harvest_abort_failed_jobs")(context, {
        "life_span": job_life_span,
        "include": include,
        "exclude": exclude
    })
    print(result)


def purge_queues():
    from ckanext.harvest.queue import purge_queues as purge

    purge()


def list_sources(all):
    if all:
        data_dict = {}
        what = "harvest source"
    else:
        data_dict = {"only_active": True}
        what = "active harvest source"

    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    sources = tk.get_action("harvest_source_list")(context, data_dict)
    output = StringIO()
    _print_harvest_sources(sources, output)
    print(_there_are(what, sources), file=output)
    return output.getvalue()


def create_job(source_id_or_name):
    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    source = tk.get_action("harvest_source_show")(context, {
        "id": source_id_or_name
    })

    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    job = tk.get_action("harvest_job_create")(context, {
        "source_id": source["id"],
        "run": True
    })

    output = StringIO()
    _print_harvest_job(job, output)
    jobs = tk.get_action("harvest_job_list")(context, {"status": u"New"})
    print(_there_are("harvest job", jobs, condition=u"New"), file=output)
    return output.getvalue()


def list_jobs():
    context = {
        "model": model,
        "user": _admin_user()["name"],
        "session": model.Session,
    }
    jobs = tk.get_action("harvest_job_list")(context, {})

    output = StringIO()
    _print_harvest_jobs(jobs, output)
    print(_there_are(what="harvest job", sequence=jobs), file=output)
    return output.getvalue()


def abort_job(job_or_source_id_or_name):
    context = {
        "model": model,
        "user": _admin_user()["name"],
        "session": model.Session,
    }
    job = tk.get_action("harvest_job_abort")(context, {
        "id": job_or_source_id_or_name
    })
    return "Job status: {0}".format(job["status"])


def gather_consumer():
    import logging
    from ckanext.harvest.queue import (
        get_gather_consumer,
        gather_callback,
        get_gather_queue_name,
    )

    logging.getLogger("amqplib").setLevel(logging.INFO)
    consumer = get_gather_consumer()
    for method, header, body in consumer.consume(
            queue=get_gather_queue_name()):
        gather_callback(consumer, method, header, body)


def fetch_consumer():
    import logging

    logging.getLogger("amqplib").setLevel(logging.INFO)
    from ckanext.harvest.queue import (
        get_fetch_consumer,
        fetch_callback,
        get_fetch_queue_name,
    )

    consumer = get_fetch_consumer()
    for method, header, body in consumer.consume(queue=get_fetch_queue_name()):
        fetch_callback(consumer, method, header, body)


def run_harvester():
    context = {
        "model": model,
        "user": _admin_user()["name"],
        "session": model.Session,
    }
    tk.get_action("harvest_jobs_run")(context, {})


def run_test_harvester(source_id_or_name, force_import):
    from ckanext.harvest import queue
    from ckanext.harvest.tests import lib
    from ckanext.harvest.logic import HarvestJobExists
    from ckanext.harvest.model import HarvestJob

    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
    }
    source = tk.get_action("harvest_source_show")(context, {
        "id": source_id_or_name
    })

    # Determine the job
    try:
        job_dict = tk.get_action("harvest_job_create")(
            context, {
                "source_id": source["id"]
            })
    except HarvestJobExists:
        running_jobs = tk.get_action("harvest_job_list")(
            context, {
                "source_id": source["id"],
                "status": "Running"
            })
        if running_jobs:
            print('\nSource "{0}" apparently has a "Running" job:\n{1}'.format(
                source.get("name") or source["id"], running_jobs))

            resp = six.moves.input("Abort it? (y/n)")
            if not resp.lower().startswith("y"):
                sys.exit(1)
            job_dict = tk.get_action("harvest_job_abort")(
                context, {
                    "source_id": source["id"]
                })
        else:
            print("Reusing existing harvest job")
            jobs = tk.get_action("harvest_job_list")(context, {
                "source_id": source["id"],
                "status": "New"
            })
            assert (len(jobs) == 1
                    ), 'Multiple "New" jobs for this source! {0}'.format(jobs)
            job_dict = jobs[0]
    job_obj = HarvestJob.get(job_dict["id"])

    if force_import:
        job_obj.force_import = force_import

    harvester = queue.get_harvester(source["source_type"])
    assert harvester, "No harvester found for type: {0}".format(
        source["source_type"])
    lib.run_harvest_job(job_obj, harvester)


def import_stage(
    source_id_or_name,
    no_join_datasets,
    harvest_object_id,
    guid,
    package_id,
    segments,
):

    if source_id_or_name:
        context = {
            "model": model,
            "session": model.Session,
            "user": _admin_user()["name"],
        }
        source = tk.get_action("harvest_source_show")(context, {
            "id": source_id_or_name
        })
        source_id = source["id"]
    else:
        source_id = None

    context = {
        "model": model,
        "session": model.Session,
        "user": _admin_user()["name"],
        "join_datasets": not no_join_datasets,
        "segments": segments,
    }

    objs_count = tk.get_action("harvest_objects_import")(
        context,
        {
            "source_id": source_id,
            "harvest_object_id": harvest_object_id,
            "package_id": package_id,
            "guid": guid,
        },
    )

    print("{0} objects reimported".format(objs_count))


def job_all():
    context = {
        "model": model,
        "user": _admin_user()["name"],
        "session": model.Session,
    }
    jobs = tk.get_action("harvest_job_create_all")(context, {})
    return "Created {0} new harvest jobs".format(len(jobs))


def reindex():
    context = {"model": model, "user": _admin_user()["name"]}
    tk.get_action("harvest_sources_reindex")(context, {})


def clean_harvest_log():
    from datetime import datetime, timedelta
    from ckantoolkit import config
    from ckanext.harvest.model import clean_harvest_log

    # Log time frame - in days
    log_timeframe = tk.asint(config.get("ckan.harvest.log_timeframe", 30))
    condition = datetime.utcnow() - timedelta(days=log_timeframe)

    # Delete logs older then the given date
    clean_harvest_log(condition=condition)


def harvesters_info():
    harvesters_info = tk.get_action("harvesters_info_show")()
    return harvesters_info


###############################################################################
#                                  Controller                                 #
###############################################################################


def _not_auth_message():
    return _('Not authorized to see this page')


def _get_source_for_job(source_id):

    try:
        context = {'model': model, 'user': tk.c.user}
        source_dict = tk.get_action('harvest_source_show')(context, {
            'id': source_id
        })
    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        return tk.abort(500, msg)

    return source_dict


def admin_view(id):
    try:
        context = {'model': model, 'user': tk.c.user}
        tk.check_access('harvest_source_update', context, {'id': id})
        harvest_source = tk.get_action('harvest_source_show')(context, {
            'id': id
        })
        return tk.render('source/admin.html',
                         extra_vars={'harvest_source': harvest_source})
    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())


def job_show_last_view(source):
    source_dict = _get_source_for_job(source)

    if not source_dict['status']['last_job']:
        return tk.abort(404, _('No jobs yet for this source'))

    return job_show_view(
        source_dict['status']['last_job']['id'],
        source_dict=source_dict,
        is_last=True,
    )


def job_show_view(id, source_dict=False, is_last=False):

    try:
        context = {'model': model, 'user': tk.c.user}
        job = tk.get_action('harvest_job_show')(context, {'id': id})
        job_report = tk.get_action('harvest_job_report')(context, {'id': id})

        if not source_dict:
            source_dict = tk.get_action('harvest_source_show')(
                context, {
                    'id': job['source_id']
                })

        return tk.render(
            'source/job/read.html',
            extra_vars={
                'harvest_source': source_dict,
                'job': job,
                'job_report': job_report,
                'is_last_job': is_last,
            },
        )

    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest job not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        return tk.abort(500, msg)


def job_list_view(source):
    try:
        context = {'model': model, 'user': tk.c.user}
        harvest_source = tk.get_action('harvest_source_show')(context, {
            'id': source
        })
        jobs = tk.get_action('harvest_job_list')(
            context, {
                'source_id': harvest_source['id']
            })

        return tk.render(
            'source/job/list.html',
            extra_vars={
                'harvest_source': harvest_source,
                'jobs': jobs
            },
        )

    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        return tk.abort(500, msg)


def about_view(id):

    try:
        context = {'model': model, 'user': tk.c.user}
        harvest_source = tk.get_action('harvest_source_show')(context, {
            'id': id
        })
        return tk.render('source/about.html',
                         extra_vars={'harvest_source': harvest_source})
    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())


def job_abort_view(source, id):

    try:
        context = {'model': model, 'user': tk.c.user}
        tk.get_action('harvest_job_abort')(context, {'id': id})
        h.flash_success(_('Harvest job stopped'))

    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest job not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        return tk.abort(500, msg)

    return h.redirect_to(
        h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=source))


def refresh_view(id):
    try:
        context = {'model': model, 'user': tk.c.user, 'session': model.Session}
        tk.get_action('harvest_job_create')(context, {
            'source_id': id,
            'run': True
        })
        h.flash_success(
            _('Harvest will start shortly. Refresh this page for updates.'))
    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except HarvestSourceInactiveError:
        h.flash_error(
            _('Cannot create new harvest jobs on inactive '
              'sources. First, please change the source status '
              'to "active".'))
    except HarvestJobExists:
        h.flash_notice(
            _('A harvest job has already been scheduled for '
              'this source'))
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        h.flash_error(msg)

    return h.redirect_to(
        h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=id))


def clear_view(id):
    try:
        context = {'model': model, 'user': tk.c.user, 'session': model.Session}
        tk.get_action('harvest_source_clear')(context, {'id': id})
        h.flash_success(_('Harvest source cleared'))
    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        h.flash_error(msg)

    return h.redirect_to(
        h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=id))


def delete_view(id):
    try:
        context = {'model': model, 'user': tk.c.user}

        context['clear_source'] = tk.request.params.get('clear',
                                                        '').lower() in (
                                                            u'true',
                                                            u'1',
                                                        )

        tk.get_action('harvest_source_delete')(context, {'id': id})

        if context['clear_source']:
            h.flash_success(_('Harvesting source successfully cleared'))
        else:
            h.flash_success(_('Harvesting source successfully inactivated'))

        return h.redirect_to(
            h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=id))
    except tk.ObjectNotFound:
        return tk.abort(404, _('Harvest source not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())


def object_show_view(id, ref_type, response):

    try:
        context = {'model': model, 'user': tk.c.user}
        if ref_type == 'object':
            obj = tk.get_action('harvest_object_show')(context, {'id': id})
        elif ref_type == 'dataset':
            obj = tk.get_action('harvest_object_show')(context, {
                'dataset_id': id
            })

        # Check content type. It will probably be either XML or JSON
        try:

            if obj['content']:
                content = obj['content']
            elif 'original_document' in obj['extras']:
                content = obj['extras']['original_document']
            else:
                return tk.abort(404, _('No content found'))
            try:
                etree.fromstring(re.sub(r'<\?xml(.*)\?>', '', content))
            except UnicodeEncodeError:
                etree.fromstring(
                    re.sub(r'<\?xml(.*)\?>', '', content.encode('utf-8')))
            response.content_type = 'application/xml; charset=utf-8'
            if '<?xml' not in content.split('\n')[0]:
                content = u'<?xml version="1.0" encoding="UTF-8"?>\n' + content

        except xml_parser_exception:
            try:
                json.loads(obj['content'])
                response.content_type = 'application/json; charset=utf-8'
            except ValueError:
                # Just return whatever it is
                pass

        response.headers['Content-Length'] = len(content)
        return (response, six.ensure_str(content))

    except tk.ObjectNotFound as e:
        return tk.abort(404, _(str(e)))
    except tk.NotAuthorized:
        return tk.abort(401, _not_auth_message())
    except Exception as e:
        msg = 'An error occurred: [%s]' % str(e)
        return tk.abort(500, msg)

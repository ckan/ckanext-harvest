# -*- coding: utf-8 -*-

import ckantoolkit as tk
from flask import Blueprint, make_response

import ckanext.harvest.utils as utils

harvester = Blueprint("harvester", __name__)


@harvester.before_request
def before_request():
    tk.c.dataset_type = utils.DATASET_TYPE_NAME


def delete(id):
    return utils.delete_view(id)


def refresh(id):
    return utils.refresh_view(id)


def admin(id):
    return utils.admin_view(id)


def about(id):
    return utils.about_view(id)


def clear(id):
    return utils.clear_view(id)


def job_list(source):
    return utils.job_list_view(source)


def job_show_last(source):
    return utils.job_show_last_view(source)


def job_show(source, id):
    return utils.job_show_view(id)


def job_abort(source, id):
    return utils.job_abort_view(source, id)


def object_show(id, ref_type):
    (response, content) = utils.object_show_view(id, ref_type, make_response())
    response.set_data(content)
    return response


harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/delete/<id>",
    view_func=delete,
)
harvester.add_url_rule("/" + utils.DATASET_TYPE_NAME + "/refresh/<id>",
                       view_func=refresh,
                       methods=(u'POST', ))
harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/admin/<id>",
    view_func=admin,
)
harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/about/<id>",
    view_func=about,
)
harvester.add_url_rule("/" + utils.DATASET_TYPE_NAME + "/clear/<id>",
                       view_func=clear,
                       methods=(u'POST', ))
harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job",
    view_func=job_list,
)
harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job/last",
    view_func=job_show_last,
)

harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job/<id>",
    view_func=job_show,
)
harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job/<id>/abort",
    view_func=job_abort,
)
harvester.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/object/<id>",
    view_func=object_show,
    defaults={"ref_type": "object"},
)
harvester.add_url_rule(
    "/dataset/harvest_object/<id>",
    view_func=object_show,
    defaults={"ref_type": "dataset"},
)


def get_blueprints():
    return [harvester]

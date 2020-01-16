# -*- coding: utf-8 -*-

from flask import Blueprint

import ckantoolkit as tk

import ckanext.harvest.utils as utils

harvest = Blueprint("harvester", __name__)


@harvest.before_request
def before_request():
    tk.c.dataset_type = utils.DATASET_TYPE_NAME


def delete(id):
    return "delete"


def refresh(id):
    return "refresh"


def admin(id):
    return utils.admin_view(id)


def about(id):
    return "about"


def clear(id):
    return "clear"


def job_list(source):
    return "job_list"


def job_show_last(source):
    return "job_show_last"


def job_show(source, id):
    return "job_show"


def job_abort(source, id):
    return "job_abort"


def object_show(id, ref_type):
    return "object_show {}".format(ref_type)


harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/delete/<id>", view_func=delete,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/refresh/<id>", view_func=refresh,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/admin/<id>", view_func=admin,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/about/<id>", view_func=about,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/clear/<id>", view_func=clear,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job", view_func=job_list,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job/last",
    view_func=job_show_last,
)

harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job/<id>", view_func=job_show,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/<source>/job/<id>/abort",
    view_func=job_abort,
)
harvest.add_url_rule(
    "/" + utils.DATASET_TYPE_NAME + "/object/<id>",
    view_func=object_show,
    defaults={"ref_type": "object"},
)
harvest.add_url_rule(
    "/dataset/harvest_object/<id>",
    view_func=object_show,
    defaults={"ref_type": "dataset"},
)


def get_blueprints():
    # import ipdb; ipdb.set_trace()
    return [harvest]

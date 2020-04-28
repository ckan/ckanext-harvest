# -*- coding: utf-8 -*-

from ckan.lib.base import BaseController, c
from ckan.common import response

import ckanext.harvest.utils as utils


class ViewController(BaseController):
    def __before__(self, action, **params):

        super(ViewController, self).__before__(action, **params)

        c.dataset_type = utils.DATASET_TYPE_NAME

    def delete(self, id):
        return utils.delete_view(id)

    def refresh(self, id):
        return utils.refresh_view(id)

    def clear(self, id):
        return utils.clear_view(id)

    def show_object(self, id, ref_type='object'):
        _, content = utils.object_show_view(id, ref_type, response)
        return content

    def show_job(self, id, source_dict=False, is_last=False):
        return utils.job_show_view(id, source_dict, is_last)

    def about(self, id):
        return utils.about_view(id)

    def admin(self, id):
        return utils.admin_view(id)

    def abort_job(self, source, id):
        return utils.job_abort_view(source, id)

    def show_last_job(self, source):
        return utils.job_show_last_view(source)

    def list_jobs(self, source):
        return utils.job_list_view(source)

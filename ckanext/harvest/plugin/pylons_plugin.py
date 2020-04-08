# -*- coding: utf-8 -*-

import ckan.plugins as p
from ckanext.harvest.utils import DATASET_TYPE_NAME

class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IRoutes, inherit=True)

    # IRoutes

    def before_map(self, map):

        # Most of the routes are defined via the IDatasetForm interface
        # (ie they are the ones for a package type)
        controller = "ckanext.harvest.controllers.view:ViewController"

        map.connect(
            "{0}_delete".format(DATASET_TYPE_NAME),
            "/" + DATASET_TYPE_NAME + "/delete/:id",
            controller=controller,
            action="delete",
        )
        map.connect(
            "{0}_refresh".format(DATASET_TYPE_NAME),
            "/" + DATASET_TYPE_NAME + "/refresh/:id",
            controller=controller,
            action="refresh",
        )
        map.connect(
            "{0}_admin".format(DATASET_TYPE_NAME),
            "/" + DATASET_TYPE_NAME + "/admin/:id",
            controller=controller,
            action="admin",
        )
        map.connect(
            "{0}_about".format(DATASET_TYPE_NAME),
            "/" + DATASET_TYPE_NAME + "/about/:id",
            controller=controller,
            action="about",
        )
        map.connect(
            "{0}_clear".format(DATASET_TYPE_NAME),
            "/" + DATASET_TYPE_NAME + "/clear/:id",
            controller=controller,
            action="clear",
        )

        map.connect(
            "harvest_job_list",
            "/" + DATASET_TYPE_NAME + "/{source}/job",
            controller=controller,
            action="list_jobs",
        )
        map.connect(
            "harvest_job_show_last",
            "/" + DATASET_TYPE_NAME + "/{source}/job/last",
            controller=controller,
            action="show_last_job",
        )
        map.connect(
            "harvest_job_show",
            "/" + DATASET_TYPE_NAME + "/{source}/job/{id}",
            controller=controller,
            action="show_job",
        )
        map.connect(
            "harvest_job_abort",
            "/" + DATASET_TYPE_NAME + "/{source}/job/{id}/abort",
            controller=controller,
            action="abort_job",
        )

        map.connect(
            "harvest_object_show",
            "/" + DATASET_TYPE_NAME + "/object/:id",
            controller=controller,
            action="show_object",
        )
        map.connect(
            "harvest_object_for_dataset_show",
            "/dataset/harvest_object/:id",
            controller=controller,
            action="show_object",
            ref_type="dataset",
        )

        return map

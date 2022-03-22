"""this plugin exists because the interface methods for
IPackageController was renamed (e.g) after_create
to after_dataset_create in CKAN 2.10.0
see more in (https://github.com/ckan/ckan/pull/6501)"""

import json
import logging
import ckan.logic as logic
import ckan.model as model
import ckan.plugins as p

from ckanext.harvest.plugin import (
    HarvestObject,
    HarvestSource,
    _create_harvest_source_object,
    _update_harvest_source_object,
    _delete_harvest_source_object,
    DATASET_TYPE_NAME,
)


log = logging.getLogger(__name__)


class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IPackageController, inherit=True)

    # IPackageController

    # CKAN >= 2.10
    def after_dataset_create(self, context, data_dict):
        if (
            "type" in data_dict
            and data_dict["type"] == DATASET_TYPE_NAME
            and not self.startup
        ):
            # Create an actual HarvestSource object
            _create_harvest_source_object(context, data_dict)

    def after_dataset_update(self, context, data_dict):
        if "type" in data_dict and data_dict["type"] == DATASET_TYPE_NAME:
            # Edit the actual HarvestSource object
            _update_harvest_source_object(context, data_dict)

    def after_dataset_delete(self, context, data_dict):

        package_dict = p.toolkit.get_action("package_show")(
            context, {"id": data_dict["id"]}
        )

        if "type" in package_dict and package_dict["type"] == DATASET_TYPE_NAME:
            # Delete the actual HarvestSource object
            _delete_harvest_source_object(context, package_dict)

    def before_dataset_search(self, search_params):
        """Prevents the harvesters being shown in dataset search results."""

        fq = search_params.get("fq", "")
        if "dataset_type:harvest" not in fq:
            fq = "{0} -dataset_type:harvest".format(search_params.get("fq", ""))
            search_params.update({"fq": fq})

        return search_params

    def before_dataset_index(self, pkg_dict):

        harvest_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.package_id == pkg_dict["id"])
            .filter(HarvestObject.current == True)
            .first()
        )  # noqa

        if harvest_object:

            data_dict = json.loads(pkg_dict["data_dict"])

            validated_data_dict = json.loads(pkg_dict["validated_data_dict"])

            harvest_extras = [
                ("harvest_object_id", harvest_object.id),
                ("harvest_source_id", harvest_object.source.id),
                ("harvest_source_title", harvest_object.source.title),
            ]

            for key, value in harvest_extras:

                # If the harvest extras are there, update them. This can
                # happen eg when calling package_update or resource_update,
                # which call package_show
                harvest_not_found = True
                harvest_not_found_validated = True
                if not data_dict.get("extras"):
                    data_dict["extras"] = []

                for e in data_dict.get("extras"):
                    if e.get("key") == key:
                        e.update({"value": value})
                        harvest_not_found = False
                if harvest_not_found:
                    data_dict["extras"].append({"key": key, "value": value})

                if not validated_data_dict.get("extras"):
                    validated_data_dict["extras"] = []

                for e in validated_data_dict.get("extras"):
                    if e.get("key") == key:
                        e.update({"value": value})
                        harvest_not_found_validated = False
                if harvest_not_found_validated:
                    validated_data_dict["extras"].append({"key": key, "value": value})

                # The commented line isn't cataloged correctly, if we pass the
                # basic key the extras are prepended and the system works as
                # expected.
                # pkg_dict['extras_{0}'.format(key)] = value
                pkg_dict[key] = value

            pkg_dict["data_dict"] = json.dumps(data_dict)
            pkg_dict["validated_data_dict"] = json.dumps(validated_data_dict)

        return pkg_dict

    def after_dataset_show(self, context, data_dict):

        if "type" in data_dict and data_dict["type"] == DATASET_TYPE_NAME:
            # This is a harvest source dataset, add extra info from the
            # HarvestSource object
            source = HarvestSource.get(data_dict["id"])
            if not source:
                log.error(
                    "Harvest source not found for dataset {0}".format(data_dict["id"])
                )
                return data_dict

            st_action_name = "harvest_source_show_status"
            try:
                status_action = p.toolkit.get_action(st_action_name)
            except KeyError:
                logic.clear_actions_cache()
                status_action = p.toolkit.get_action(st_action_name)

            data_dict["status"] = status_action(context, {"id": source.id})

        return data_dict

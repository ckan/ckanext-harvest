# -*- coding: utf-8 -*-

import ckan.plugins as p
import ckanext.harvest.cli as cli
import ckanext.harvest.views as views

class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IClick)
    p.implements(p.IBlueprint)

    # IClick

    def get_commands(self):
        return cli.get_commands()

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprints()

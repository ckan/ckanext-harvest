# -*- coding: utf-8 -*-

import ckan.plugins as p
import ckanext.harvest.cli as cli

class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IClick)

    # IClick

    def get_commands(self):
        return cli.get_commands()

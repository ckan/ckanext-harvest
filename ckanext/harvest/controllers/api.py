from ckan.lib.helpers import json
import ckan.lib.helpers as h
from ckan.lib.base import c, g, request, \
                          response, session, render, config, abort, redirect
from ckan.controllers.rest import BaseApiController

from ckan.model import Session

from ckanext.harvest.lib import get_srid


class ApiController(BaseApiController):
    
    db_srid = int(config.get('ckan.harvesting.srid', '4258'))

    def spatial_query(self):
        if not 'bbox' in request.params:
            abort(400)
        
        bbox = request.params['bbox'].split(',')
        if len(bbox) is not 4:
            abort(400)
        
        minx = float(bbox[0])
        miny = float(bbox[1])
        maxx = float(bbox[2])
        maxy = float(bbox[3])

        params = {'minx':minx,'miny':miny,'maxx':maxx,'maxy':maxy,'db_srid':self.db_srid}
        
        srid = get_srid(request.params.get('crs')) if 'crs' in request.params else None
        if srid and srid != self.db_srid:
            # The input bounding box is defined in another projection, we need
            # to transform it
            statement = """SELECT package_id FROM package_extent WHERE 
                            ST_Intersects(
                                ST_Transform(
                                    ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                        %(maxx)s %(miny)s,
                                                        %(maxx)s %(maxy)s,
                                                        %(minx)s %(maxy)s,
                                                        %(minx)s %(miny)s))',%(srid)s),
                                    %(db_srid)s)
                                ,the_geom)"""
            params.update({'srid': srid})
        else:
            statement = """SELECT package_id FROM package_extent WHERE 
                            ST_Intersects(
                                ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                        %(maxx)s %(miny)s,
                                                        %(maxx)s %(maxy)s,
                                                        %(minx)s %(maxy)s,
                                                        %(minx)s %(miny)s))',%(db_srid)s),
                                the_geom)"""
        conn = Session.connection()
        rows = conn.execute(statement,params)
        ids = [row['package_id'] for row in rows]
    
        output = dict(count=len(ids),results=ids)

        return self._finish_ok(output)



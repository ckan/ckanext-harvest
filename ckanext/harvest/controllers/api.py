from ckan.lib.helpers import json
import ckan.lib.helpers as h
from ckan.lib.base import c, g, request, \
                          response, session, render, config, abort, redirect
from ckan.controllers.rest import BaseApiController


from ckan.model import Session


class ApiController(BaseApiController):
    def _get_srid(self,crs):
        """Returns the SRID for the provided CRS definition
            The CRS can be defined in the following formats
            - urn:ogc:def:crs:EPSG::4258
            - EPSG:4258
            - 4258
           """

        if ':' in crs:
            crs = crs.split(':')
            srid = crs[len(crs)-1]
        else:
           srid = crs

        return int(srid)


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

        params = {'minx':minx,'miny':miny,'maxx':maxx,'maxy':maxy}

        if 'crs' in request.params:
            # The input bounding box is defined in another projection, we need
            # to transform it
            srid = self._get_srid(request.params['crs'])

            statement = """SELECT package_id FROM package_extent WHERE 
                            ST_Intersects(
                                ST_Transform(
                                    ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                        %(maxx)s %(miny)s,
                                                        %(maxx)s %(maxy)s,
                                                        %(minx)s %(maxy)s,
                                                        %(minx)s %(miny)s))',%(srid)s),
                                    4258)
                                ,the_geom)"""
            params.update({'srid': srid})
        else:
            statement = """SELECT package_id FROM package_extent WHERE 
                            ST_Intersects(
                                ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                        %(maxx)s %(miny)s,
                                                        %(maxx)s %(maxy)s,
                                                        %(minx)s %(maxy)s,
                                                        %(minx)s %(miny)s))',4258),
                                the_geom)"""
        try:
            conn = Session.connection()
            rows = conn.execute(statement,params)
            ids = [row['package_id'] for row in rows]
        
            output = dict(count=len(ids),results=ids)

            return self._finish_ok(output)
        except:    
            abort(500)



from ckan.model import Session
from ckan.lib.base import config


log = __import__("logging").getLogger(__name__)

def get_srid(crs):
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

def save_extent(package,extent=False):
    '''Updates the package extent in the package_extent geometry column
       If no extent provided (as a dict with minx,miny,maxx,maxy and srid keys),
       the values stored in the package extras are used'''

    db_srid = int(config.get('ckan.harvesting.srid', '4258'))
    conn = Session.connection()

    srid = None
    if extent:
        minx = extent['minx'] 
        miny = extent['miny']
        maxx = extent['maxx']
        maxy = extent['maxy']
        if 'srid' in extent:
            srid = extent['srid'] 
    else:
        minx = float(package.extras.get('bbox-east-long'))
        miny = float(package.extras.get('bbox-south-lat'))
        maxx = float(package.extras.get('bbox-west-long'))
        maxy = float(package.extras.get('bbox-north-lat'))
        crs = package.extras.get('spatial-reference-system')
        if crs:
            srid = get_srid(crs) 
    try:
        
        # Check if extent already exists
        rows = conn.execute('SELECT package_id FROM package_extent WHERE package_id = %s',package.id).fetchall()
        update =(len(rows) > 0)
        
        params = {'id':package.id, 'minx':minx,'miny':miny,'maxx':maxx,'maxy':maxy, 'db_srid': db_srid}
        
        if update:
            # Update
            if srid and srid != db_srid:
                # We need to reproject the input geometry
                statement = """UPDATE package_extent SET 
                                the_geom = ST_Transform(
                                            ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                            %(maxx)s %(miny)s,
                                                            %(maxx)s %(maxy)s,
                                                            %(minx)s %(maxy)s,
                                                            %(minx)s %(miny)s))',%(srid)s),
                                            %(db_srid)s)
                                WHERE package_id = %(id)s
                                """
                params.update({'srid': srid})
            else:
                statement = """UPDATE package_extent SET 
                                the_geom = ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                            %(maxx)s %(miny)s,
                                                            %(maxx)s %(maxy)s,
                                                            %(minx)s %(maxy)s,
                                                            %(minx)s %(miny)s))',%(db_srid)s)
                                WHERE package_id = %(id)s
                                """
            msg = 'Updated extent for package %s' 
        else:
            # Insert
            if srid and srid != db_srid:
                # We need to reproject the input geometry
                statement = """INSERT INTO package_extent (package_id,the_geom) VALUES (
                                %(id)s,
                                ST_Transform(
                                    ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                            %(maxx)s %(miny)s,
                                                            %(maxx)s %(maxy)s,
                                                            %(minx)s %(maxy)s,
                                                            %(minx)s %(miny)s))',%(srid)s),
                                        %(db_srid))
                                        )"""
                params.update({'srid': srid})          
            else:
                statement = """INSERT INTO package_extent (package_id,the_geom) VALUES (
                                %(id)s,
                                ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                            %(maxx)s %(miny)s,
                                                            %(maxx)s %(maxy)s,
                                                            %(minx)s %(maxy)s,
                                                            %(minx)s %(miny)s))',%(db_srid)s))"""
            msg = 'Created new extent for package %s' 

        conn.execute(statement,params)

        Session.commit()
        log.info(msg, package.id)
        return package
    except:
        log.error('An error occurred when saving the extent for package %s',package.id)
        raise Exception

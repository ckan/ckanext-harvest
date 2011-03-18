from ckan.model import Session

log = __import__("logging").getLogger(__name__)

def save_extent(package,extent=False):
    '''Updates the package extent in the package_extent geometry column
       If no extent provided (as a dict with minx,miny,maxx,maxy and srid keys),
       the values stored in the package extras are used'''
    #TODO: configure SRID
    conn = Session.connection()
    if extent:
        minx = extent['minx'] 
        miny = extent['miny']
        maxx = extent['maxx']
        maxy = extent['maxy']
    else:
        minx = float(package.extras.get('bbox-east-long'))
        miny = float(package.extras.get('bbox-south-lat'))
        maxx = float(package.extras.get('bbox-west-long'))
        maxy = float(package.extras.get('bbox-north-lat'))
    
    try:
        
        # Check if extent already exists
        rows = conn.execute('SELECT package_id FROM package_extent WHERE package_id = %s',package.id).fetchall()
        update =(len(rows) > 0)

        if update:
            # Update
            statement = """UPDATE package_extent SET 
                            the_geom = ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                        %(maxx)s %(miny)s,
                                                        %(maxx)s %(maxy)s,
                                                        %(minx)s %(maxy)s,
                                                        %(minx)s %(miny)s))',4258)
                            WHERE package_id = %(id)s
                            """
            msg = 'Updated extent for package %s' 
        else:
            # Insert
            statement = """INSERT INTO package_extent (package_id,the_geom) VALUES (
                            %(id)s,
                            ST_GeomFromText('POLYGON ((%(minx)s %(miny)s, 
                                                        %(maxx)s %(miny)s,
                                                        %(maxx)s %(maxy)s,
                                                        %(minx)s %(maxy)s,
                                                        %(minx)s %(miny)s))',4258))"""
            msg = 'Created new extent for package %s' 

        conn.execute(statement,{'id':package.id, 'minx':minx,'miny':miny,'maxx':maxx,'maxy':maxy})

        Session.commit()
        log.info(msg, package.id)
    except:
        log.error('An error occurred when saving the extent for package %s',package.id)
    finally:
        return package


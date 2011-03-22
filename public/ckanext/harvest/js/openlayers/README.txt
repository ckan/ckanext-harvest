This is a custom build of the OpenLayers Javascript mapping library,
slimmed down to only the features we need.

The file ckan.cfg contains the build profile used to build OpenLayers.
In order to add more functionality, new classes must be added in the
build profile, and then run the build command from the OpenLayers
distribution:

1. svn co http://svn.openlayers.org/trunk/openlayers

2. Modify ckan.cfg

3. Go to build/ and execute::

    python build.py {path-to-ckan.cfg} {output-file}


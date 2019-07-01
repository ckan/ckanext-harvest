#!/bin/sh -e

nosetests --verbose --ckan --with-pylons=subdir/test-core.ini ckanext/harvest

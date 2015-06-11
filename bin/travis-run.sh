#!/bin/sh -e

nosetests --ckan --nologcapture --with-pylons=subdir/test-core.ini ckanext/harvest

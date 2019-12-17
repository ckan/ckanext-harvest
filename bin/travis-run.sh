#!/bin/sh -e

nosetests --ckan --nologcapture --with-pylons=subdir/test-core.ini -v ckanext/harvest

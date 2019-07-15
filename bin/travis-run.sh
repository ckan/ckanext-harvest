#!/bin/sh -e

nosetests -s --verbose --ckan --with-pylons=subdir/test-core.ini ckanext/harvest

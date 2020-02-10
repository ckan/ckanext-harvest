#!/bin/sh -e

nosetests --ckan --nologcapture --with-pylons=subdir/test-core-nose.ini -v ckanext/harvest/tests/nose

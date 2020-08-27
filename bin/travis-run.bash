#!/bin/bash
set -e

pytest --ckan-ini=subdir/test.ini --cov=ckanext.harvest --disable-warnings ckanext/harvest/tests

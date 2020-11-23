#!/bin/bash
set -e

echo "This is travis-build.bash..."
echo "Targetting CKAN $CKANVERSION on Python $TRAVIS_PYTHON_VERSION"
if [ $CKANVERSION == 'master' ]
then
    export CKAN_MINOR_VERSION=100
else
    export CKAN_MINOR_VERSION=${CKANVERSION##*.}
fi

export PYTHON_MAJOR_VERSION=${TRAVIS_PYTHON_VERSION%.*}

echo "Installing CKAN and its Python dependencies..."

echo "Setting up Solr..."
docker run --name ckan-solr -p 8983:8983 -d openknowledge/ckan-solr-dev:$CKANVERSION

echo "Setting up Postgres..."
pg_lsclusters

echo "travis-build.bash is done."

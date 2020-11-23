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
git clone https://github.com/ckan/ckan
cd ckan
if [ $CKANVERSION == 'master' ]
then
    echo "CKAN version: master"
else
    CKAN_TAG=$(git tag | grep ^ckan-$CKANVERSION | sort --version-sort | tail -n 1)
    git checkout $CKAN_TAG
    echo "CKAN version: ${CKAN_TAG#ckan-}"
fi

echo "Installing the recommended setuptools requirement"
if [ -f requirement-setuptools.txt ]
then
    pip install -r requirement-setuptools.txt
fi

python setup.py develop

if (( $CKAN_MINOR_VERSION >= 9 )) && (( $PYTHON_MAJOR_VERSION == 2 ))
then
    pip install -r requirements-py2.txt
else
    pip install -r requirements.txt
fi

pip install -r dev-requirements.txt
cd -

echo "Setting up Solr..."
docker run --name ckan-solr -p 8983:8983 -d openknowledge/ckan-solr-dev:$CKANVERSION

echo "Setting up Postgres..."
pg_lsclusters

echo "travis-build.bash is done."

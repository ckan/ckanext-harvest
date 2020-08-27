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

echo "Setting up Postgres"
echo "Starting Postgres $PGVERSION"
service postgresql stop && service postgresql start $PGVERSION
service postgresql status


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
docker run --name ckan-solr -p 8983:8983 -d openknowledge/ckan-solr:6.6
sed -i -e 's/solr_url.*/solr_url = http:\/\/127.0.0.1:8983\/solr\/ckan-2.8/' ckan/test-core.ini

echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c "CREATE USER datastore_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER ckan_default;'
sudo -u postgres psql -c 'CREATE DATABASE datastore_test WITH OWNER ckan_default;'

echo "Initialising the database..."
cd ckan
if (( $CKAN_MINOR_VERSION >= 9 ))
then
    ckan -c test-core.ini db init
else
    paster db init -c test-core.ini
fi
cd -

echo "Installing ckanext-harvest and its requirements..."
pip install -r pip-requirements.txt
pip install -r dev-requirements.txt

python setup.py develop


echo "Moving test.ini into a subdir... (because the core ini file is referenced as ../ckan/test-core.ini)"
mkdir subdir
mv test.ini subdir


if (( $CKAN_MINOR_VERSION >= 9 ))
then
    ckan -c subdir/test.ini harvester initdb
else
    paster harvester initdb -c subdir/test.ini
fi


echo "travis-build.bash is done."

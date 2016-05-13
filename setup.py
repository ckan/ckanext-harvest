from setuptools import setup, find_packages
import sys, os
import codecs
import os.path

version = '0.2'

# Read requirements from {pip,dev}-requirements.txt
HERE = os.path.dirname(__file__)
PIP_REQUIREMENTS_TXT = os.path.join(HERE, 'pip-requirements.txt')
DEV_REQUIREMENTS_TXT = os.path.join(HERE, 'dev-requirements.txt')
with codecs.open(PIP_REQUIREMENTS_TXT, encoding='utf8') as f:
    install_requires = f.readlines()
with codecs.open(DEV_REQUIREMENTS_TXT, encoding='utf8') as f:
    tests_require = f.readlines()


setup(
	name='ckanext-harvest',
	version=version,
	description="Harvesting interface plugin for CKAN",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='CKAN',
	author_email='ckan@okfn.org',
	url='http://ckan.org/wiki/Extensions',
	license='mit',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.harvest'],
	include_package_data=True,
	zip_safe=False,
	install_requires=install_requires,
	tests_require=tests_require,
	test_suite = 'nose.collector',
	entry_points=\
	"""
    [ckan.plugins]
	# Add plugins here, eg
	harvest=ckanext.harvest.plugin:Harvest
	ckan_harvester=ckanext.harvest.harvesters:CKANHarvester
    [ckan.test_plugins]
	test_harvester=ckanext.harvest.tests.test_queue:MockHarvester
	test_action_harvester=ckanext.harvest.tests.test_action:MockHarvesterForActionTests
	[paste.paster_command]
	harvester = ckanext.harvest.commands.harvester:Harvester
    [babel.extractors]
    ckan = ckan.lib.extract:extract_ckan
	""",
        message_extractors={
            'ckanext': [
                ('**.py', 'python', None),
                ('**.js', 'javascript', None),
                ('**/templates/**.html', 'ckan', None),
            ],
        }
)

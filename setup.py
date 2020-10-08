from setuptools import setup, find_packages

version = '1.3.2'

setup(
    name='ckanext-harvest',
    version=version,
    description="Harvesting interface plugin for CKAN",
    long_description="""\
    """,
    classifiers=[],
    keywords='',
    author='CKAN',
    author_email='ckan@okfn.org',
    url='https://github.com/ckan/ckanext-harvest',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
            # dependencies are specified in pip-requirements.txt
            # instead of here
    ],
    tests_require=[
        'nose',
        'mock',
    ],
    test_suite='nose.collector',
    entry_points="""
        [ckan.plugins]
            # Add plugins here, eg
            harvest=ckanext.harvest.plugin:Harvest
            ckan_harvester=ckanext.harvest.harvesters:CKANHarvester

            # Test plugins

            test_harvester=ckanext.harvest.tests.test_queue:MockHarvester
            test_harvester2=ckanext.harvest.tests.test_queue2:MockHarvester
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

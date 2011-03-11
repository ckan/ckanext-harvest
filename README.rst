=============================================
ckanext-harvesting - CSW harvesting extension
=============================================

This extension will contain all harvesting related code, now present
in ckan core, ckanext-dgu and ckanext-csw.

Dependencies
============

You will need ckan installed, as well as the ckanext-dgu and ckanext-csw
plugins activated.


Configuration
=============

The extension needs a user with sysadmin privileges to perform the 
harvesting jobs. The user's API key must be defined in the CKAN
configuration file (.ini) in the [app:main] section.

ckan.harvesting.api_key = 4e1dac58-f642-4e54-bbc4-3ea262271fe2

The API URL used can be also defined in the ini file (it defaults to 
http://localhost:5000/).

ckan.api_url = <api_url>
 




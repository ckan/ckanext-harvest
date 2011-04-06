from ckan.plugins.interfaces import Interface

class IHarvester(Interface):
    '''
    Common harvesting interface

    '''

    def get_type(self):
        '''
        Plugins must provide this method, which will return a string with the
        Harvester type implemented by the plugin (e.g ``CSW``,``INSPIRE``, etc).
        This will ensure that they only receive Harvest Jobs and Objects
        relevant to them.

        returns: A string with the harvester type
        '''


    def gather_stage(self, harvest_job):
        '''
        The gather stage will recieve a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - update the fetch_started, fetch_finished and retry_times as
              necessary.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g 
              create a CKAN package).
            - creatingg the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwisie.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''


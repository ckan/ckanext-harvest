import logging
import datetime

from carrot.connection import BrokerConnection
from carrot.messaging import Publisher
from carrot.messaging import Consumer

from ckan.lib.base import config
from ckan.plugins import PluginImplementations

from ckanext.harvest.model import HarvestJob, HarvestObject,HarvestGatherError
from ckanext.harvest.interfaces import IHarvester

log = logging.getLogger(__name__)
assert not log.disabled

__all__ = ['get_gather_publisher', 'get_gather_consumer', \
           'get_fetch_publisher', 'get_fetch_consumer']

PORT = 5672
USERID = 'guest'
PASSWORD = 'guest'
HOSTNAME = 'localhost'
VIRTUAL_HOST = '/'

# settings for AMQP
EXCHANGE_TYPE = 'direct'
EXCHANGE_NAME = 'ckan.harvest'

def get_carrot_connection():
    backend = config.get('ckan.harvest.mq.library', 'pyamqplib')
    log.debug("Carrot connection using %s backend" % backend)
    try:
        port = int(config.get('ckan.harvest.mq.port', PORT))
    except ValueError:
        port = PORT
    userid = config.get('ckan.harvest.mq.user_id', USERID)
    password = config.get('ckan.harvest.mq.password', PASSWORD)
    hostname = config.get('ckan.harvest.mq.hostname', HOSTNAME)
    virtual_host = config.get('ckan.harvest.mq.virtual_host', VIRTUAL_HOST)

    backend_cls = 'carrot.backends.%s.Backend' % backend
    return BrokerConnection(hostname=hostname, port=port,
                            userid=userid, password=password,
                            virtual_host=virtual_host,
                            backend_cls=backend_cls)

def get_publisher(routing_key):
    return Publisher(connection=get_carrot_connection(),
                     exchange=EXCHANGE_NAME,
                     exchange_type=EXCHANGE_TYPE,
                     routing_key=routing_key)

def get_consumer(queue_name, routing_key):
    return Consumer(connection=get_carrot_connection(),
                    queue=queue_name,
                    routing_key=routing_key,
                    exchange=EXCHANGE_NAME,
                    exchange_type=EXCHANGE_TYPE,
                    durable=True, auto_delete=False)


def gather_callback(message_data,message):
    try:
        id = message_data['harvest_job_id']
        log.debug('Received harvest job id: %s' % id)

        # Get a publisher for the fetch queue
        publisher = get_fetch_publisher()

        try:
            job = HarvestJob.get(id)
        except:
            log.error('Harvest job does not exist: %s' % id)
        else:
            # Send the harvest job to the plugins that implement
            # the Harvester interface, only if the source type
            # matches
            harvester_found = False
            for harvester in PluginImplementations(IHarvester):
                if harvester.info()['name'] == job.source.type:
                    harvester_found = True
                    # Get a list of harvest object ids from the plugin
                    job.gather_started = datetime.datetime.now()
                    harvest_object_ids = harvester.gather_stage(job)
                    job.gather_finished = datetime.datetime.now()
                    job.save()
                    log.debug('Received from plugin''s gather_stage: %r' % harvest_object_ids)
                    if harvest_object_ids and len(harvest_object_ids) > 0:
                        for id in harvest_object_ids:
                            # Send the id to the fetch queue
                            publisher.send({'harvest_object_id':id})
                            log.debug('Sent object %s to the fetch queue' % id)

            if not harvester_found:
                msg = 'No harvester could be found for source type %s' % job.source.type
                err = HarvestGatherError(message=msg,job=job)
                err.save()
                log.error(msg)

            job.status = u'Finished'
            job.save()

        finally:
            publisher.close()

    except KeyError:
        log.error('No harvest job id received')
    finally:
        message.ack()


def fetch_callback(message_data,message):
    try:
        id = message_data['harvest_object_id']
        log.info('Received harvest object id: %s' % id)

        try:
            obj = HarvestObject.get(id)
        except:
            log.error('Harvest object does not exist: %s' % id)
        else:
            # Send the harvest object to the plugins that implement
            # the Harvester interface, only if the source type
            # matches
            for harvester in PluginImplementations(IHarvester):
                if harvester.info()['name'] == obj.source.type:

                    # See if the plugin can fetch the harvest object
                    obj.fetch_started = datetime.datetime.now()
                    success = harvester.fetch_stage(obj)
                    obj.fetch_finished = datetime.datetime.now()
                    obj.save()
                    #TODO: retry times?
                    if success:
                        # If no errors where found, call the import method
                        harvester.import_stage(obj)



    except KeyError:
        log.error('No harvest object id received')
    finally:
        message.ack()

def get_gather_consumer():
    consumer = get_consumer('ckan.harvest.gather','harvest_job_id')
    consumer.register_callback(gather_callback)
    log.debug('Gather queue consumer registered')
    return consumer

def get_fetch_consumer():
    consumer = get_consumer('ckan.harvert.fetch','harvest_object_id')
    consumer.register_callback(fetch_callback)
    log.debug('Fetch queue consumer registered')
    return consumer

def get_gather_publisher():
    return get_publisher('harvest_job_id')

def get_fetch_publisher():
    return get_publisher('harvest_object_id')

# Get a publisher for the fetch queue
#fetch_publisher = get_fetch_publisher()


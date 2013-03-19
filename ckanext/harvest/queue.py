import logging
import datetime
import json

import pika

from ckan.lib.base import config
from ckan.plugins import PluginImplementations
from ckan import model

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

def get_connection():

    try:
        port = int(config.get('ckan.harvest.mq.port', PORT))
    except ValueError:
        port = PORT
    userid = config.get('ckan.harvest.mq.user_id', USERID)
    password = config.get('ckan.harvest.mq.password', PASSWORD)
    hostname = config.get('ckan.harvest.mq.hostname', HOSTNAME)
    virtual_host = config.get('ckan.harvest.mq.virtual_host', VIRTUAL_HOST)

    credentials = pika.PlainCredentials(userid, password)
    parameters = pika.ConnectionParameters(host=hostname,
                                           port=port,
                                           virtual_host=virtual_host,
                                           credentials=credentials,
                                           frame_max=10000)
    log.debug("pika connection using %s" % parameters.__dict__)

    return pika.BlockingConnection(parameters)

def purge_queues():
    connection = get_connection()
    channel = connection.channel()
    channel.queue_purge(queue='ckan.harvest.gather')
    channel.queue_purge(queue='ckan.harvest.fetch')

class Publisher(object):
    def __init__(self, connection, channel, exchange, routing_key):
        self.connection = connection
        self.channel = channel
        self.exchange = exchange
        self.routing_key = routing_key
    def send(self, body, **kw):
        return self.channel.basic_publish(self.exchange,
                                          self.routing_key,
                                          json.dumps(body),
                                          properties=pika.BasicProperties(
                                             delivery_mode = 2, # make message persistent
                                          ),
                                          **kw)
    def close(self):
        self.connection.close()

def get_publisher(routing_key):
    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)
    return Publisher(connection,
                     channel,
                     EXCHANGE_NAME,
                     routing_key=routing_key)


def get_consumer(queue_name, routing_key):

    connection = get_connection()
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(queue=queue_name, exchange=EXCHANGE_NAME, routing_key=routing_key)

    return channel


def gather_callback(channel, method, header, body):
    try:
        id = json.loads(body)['harvest_job_id']
        log.debug('Received harvest job id: %s' % id)
    except KeyError:
        log.error('No harvest job id received')
        channel.basic_ack(method.delivery_tag)
        return False

    # Get a publisher for the fetch queue
    publisher = get_fetch_publisher()

    job = HarvestJob.get(id)

    if not job:
        log.error('Harvest job does not exist: %s' % id)
        channel.basic_ack(method.delivery_tag)
        return False

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

            if not isinstance(harvest_object_ids, list):
                log.error('Gather stage failed')
                publisher.close()
                channel.basic_ack(method.delivery_tag)
                return False

            if len(harvest_object_ids) == 0:
                log.info('No harvest objects to fetch')
                publisher.close()
                channel.basic_ack(method.delivery_tag)
                return False

            log.debug('Received from plugin gather_stage: {0} objects (first: {1} last: {2})'.format(
                        len(harvest_object_ids), harvest_object_ids[:1], harvest_object_ids[-1:]))
            for id in harvest_object_ids:
                # Send the id to the fetch queue
                publisher.send({'harvest_object_id':id})
            log.debug('Sent {0} objects to the fetch queue'.format(len(harvest_object_ids)))

    if not harvester_found:
        msg = 'No harvester could be found for source type %s' % job.source.type
        err = HarvestGatherError(message=msg,job=job)
        err.save()
        log.error(msg)

    model.Session.remove()
    publisher.close()
    channel.basic_ack(method.delivery_tag)


def fetch_callback(channel, method, header, body):
    try:
        id = json.loads(body)['harvest_object_id']
        log.info('Received harvest object id: %s' % id)
    except KeyError:
        log.error('No harvest object id received')
        channel.basic_ack(method.delivery_tag)
        return False


    obj = HarvestObject.get(id)
    if not obj:
        log.error('Harvest object does not exist: %s' % id)
        channel.basic_ack(method.delivery_tag)
        return False

    obj.retry_times += 1
    obj.save()

    if obj.retry_times >= 5:
        obj.state = "ERROR"
        obj.save()
        log.error('Too many consecutive retries for object {0}'.format(obj.id))
        channel.basic_ack(method.delivery_tag)
        return False

    # Send the harvest object to the plugins that implement
    # the Harvester interface, only if the source type
    # matches
    for harvester in PluginImplementations(IHarvester):
        if harvester.info()['name'] == obj.source.type:
            fetch_and_import_stages(harvester, obj)

    model.Session.remove()
    channel.basic_ack(method.delivery_tag)

def fetch_and_import_stages(harvester, obj):
    obj.fetch_started = datetime.datetime.now()
    obj.state = "FETCH"
    obj.save()
    success_fetch = harvester.fetch_stage(obj)
    obj.fetch_finished = datetime.datetime.now()
    obj.save()
    if success_fetch:
        # If no errors where found, call the import method
        obj.import_started = datetime.datetime.now()
        obj.state = "IMPORT"
        obj.save()
        success_import = harvester.import_stage(obj)
        obj.import_finished = datetime.datetime.now()
        if success_import:
            obj.state = "COMPLETE"
        else:
            obj.state = "ERROR"
        obj.save()
    else:
        obj.state = "ERROR"
        obj.save()
    if obj.report_status:
        return
    if obj.state == 'ERROR':
        obj.report_status = 'errored'
    elif obj.current == False:
        obj.report_status = 'deleted'
    elif len(model.Session.query(HarvestObject)
           .filter_by(package_id = obj.package_id)
           .limit(2)
           .all()) == 2:
        obj.report_status = 'updated'
    else:
        obj.report_status = 'added'
    obj.save()

def get_gather_consumer():
    consumer = get_consumer('ckan.harvest.gather','harvest_job_id')
    log.debug('Gather queue consumer registered')
    return consumer

def get_fetch_consumer():
    consumer = get_consumer('ckan.harvest.fetch','harvest_object_id')
    log.debug('Fetch queue consumer registered')
    return consumer

def get_gather_publisher():
    return get_publisher('harvest_job_id')

def get_fetch_publisher():
    return get_publisher('harvest_object_id')

# Get a publisher for the fetch queue
#fetch_publisher = get_fetch_publisher()


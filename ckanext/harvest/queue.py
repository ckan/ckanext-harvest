import logging
import datetime
import json

import pika
import sqlalchemy

from ckan.lib.base import config
from ckan.plugins import PluginImplementations
from ckan import model

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
from ckanext.harvest.interfaces import IHarvester

log = logging.getLogger(__name__)
assert not log.disabled

__all__ = ['get_gather_publisher', 'get_gather_consumer',
           'get_fetch_publisher', 'get_fetch_consumer',
           'get_harvester']

PORT = 5672
USERID = 'guest'
PASSWORD = 'guest'
HOSTNAME = 'localhost'
VIRTUAL_HOST = '/'
MQ_TYPE = 'amqp'
REDIS_PORT = 6379
REDIS_DB = 0

# settings for AMQP
EXCHANGE_TYPE = 'direct'
EXCHANGE_NAME = 'ckan.harvest'

def get_connection():
    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)
    if backend in ('amqp', 'ampq'):  # "ampq" is for compat with old typo
        return get_connection_amqp()
    if backend == 'redis':
        return get_connection_redis()
    raise Exception('not a valid queue type %s' % backend)

def get_connection_amqp():
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

def get_connection_redis():
    import redis
    return redis.StrictRedis(host=config.get('ckan.harvest.mq.hostname', HOSTNAME),
                          port=int(config.get('ckan.harvest.mq.port', REDIS_PORT)),
                          db=int(config.get('ckan.harvest.mq.redis_db', REDIS_DB)))


def get_gather_queue_name():
    return 'ckan.harvest.{0}.gather'.format(config.get('ckan.site_id',
                                                       'default'))


def get_fetch_queue_name():
    return 'ckan.harvest.{0}.fetch'.format(config.get('ckan.site_id',
                                                      'default'))


def get_gather_routing_key():
    return '{0}:harvest_job_id'.format(config.get('ckan.site_id',
                                                  'default'))


def get_fetch_routing_key():
    return '{0}:harvest_object_id'.format(config.get('ckan.site_id',
                                                     'default'))


def purge_queues():

    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)
    connection = get_connection()
    if backend in ('amqp', 'ampq'):
        channel = connection.channel()
        channel.queue_purge(queue=get_gather_queue_name())
        log.info('AMQP queue purged: %s', get_gather_queue_name())
        channel.queue_purge(queue=get_fetch_queue_name())
        log.info('AMQP queue purged: %s', get_fetch_queue_name())
        return
    if backend == 'redis':
        connection.flushdb()
        log.info('Redis database flushed')

def resubmit_jobs():
    '''
    Examines the fetch and gather queues for items that are suspiciously old.
    These are removed from the queues and placed back on them afresh, to ensure
    the fetch & gather consumers are triggered to process it.
    '''
    if config.get('ckan.harvest.mq.type') != 'redis':
        return
    redis = get_connection()

    # fetch queue
    harvest_object_pending = redis.keys(get_fetch_routing_key() + ':*')
    for key in harvest_object_pending:
        date_of_key = datetime.datetime.strptime(redis.get(key),
                                                 "%Y-%m-%d %H:%M:%S.%f")
        # 3 minutes for fetch and import max
        if (datetime.datetime.now() - date_of_key).seconds > 180:
            redis.rpush(get_fetch_routing_key(),
                json.dumps({'harvest_object_id': key.split(':')[-1]})
            )
            redis.delete(key)

    # gather queue
    harvest_jobs_pending = redis.keys(get_gather_routing_key() + ':*')
    for key in harvest_jobs_pending:
        date_of_key = datetime.datetime.strptime(redis.get(key),
                                                 "%Y-%m-%d %H:%M:%S.%f")
        # 3 hours for a gather
        if (datetime.datetime.now() - date_of_key).seconds > 7200:
            redis.rpush(get_gather_routing_key(),
                json.dumps({'harvest_job_id': key.split(':')[-1]})
            )
            redis.delete(key)

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

class RedisPublisher(object):
    def __init__(self, redis, routing_key):
        self.redis = redis ## not used
        self.routing_key = routing_key
    def send(self, body, **kw):
        value = json.dumps(body)
        # remove if already there
        if self.routing_key == get_gather_routing_key():
            self.redis.lrem(self.routing_key, 0, value)
        self.redis.rpush(self.routing_key, value)

    def close(self):
        return

def get_publisher(routing_key):
    connection = get_connection()
    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)
    if backend in ('amqp', 'ampq'):
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)
        return Publisher(connection,
                         channel,
                         EXCHANGE_NAME,
                         routing_key=routing_key)
    if backend == 'redis':
        return RedisPublisher(connection, routing_key)


class FakeMethod(object):
    ''' This is to act like the method returned by AMQP'''
    def __init__(self, message):
        self.delivery_tag = message


class RedisConsumer(object):
    def __init__(self, redis, routing_key):
        self.redis = redis
        # Routing keys are constructed with {site-id}:{message-key}, eg:
        # default:harvest_job_id or default:harvest_object_id
        self.routing_key = routing_key
        # Message keys are harvest_job_id for the gather consumer and
        # harvest_object_id for the fetch consumer
        self.message_key = routing_key.split(':')[-1]

    def consume(self, queue):
        while True:
            key, body = self.redis.blpop(self.routing_key)
            self.redis.set(self.persistance_key(body),
                           str(datetime.datetime.now()))
            yield (FakeMethod(body), self, body)

    def persistance_key(self, message):
        # Persistance keys are constructed with
        # {site-id}:{message-key}:{object-id}, eg:
        # default:harvest_job_id:804f114a-8f68-4e7c-b124-3eb00f66202e
        message = json.loads(message)
        return self.routing_key + ':' + message[self.message_key]

    def basic_ack(self, message):
        self.redis.delete(self.persistance_key(message))

    def queue_purge(self, queue):
        self.redis.flushdb()

    def basic_get(self, queue):
        body = self.redis.lpop(self.routing_key)
        return (FakeMethod(body), self, body)


def get_consumer(queue_name, routing_key):

    connection = get_connection()
    backend = config.get('ckan.harvest.mq.type', MQ_TYPE)

    if backend in ('amqp', 'ampq'):
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, durable=True)
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(queue=queue_name, exchange=EXCHANGE_NAME, routing_key=routing_key)
        return channel
    if backend == 'redis':
        return RedisConsumer(connection, routing_key)


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

    try:
        job = HarvestJob.get(id)
    except sqlalchemy.exc.OperationalError, e:
        # Occasionally we see: sqlalchemy.exc.OperationalError
        # "SSL connection has been closed unexpectedly"
        log.exception(e)
        log.error('Connection Error during gather of job %s: %r %r',
                  id, e, e.args)
        # By not sending the ack, it will be retried later.
        # Try to clear the issue with a remove.
        model.Session.remove()
        return
    if not job:
        log.error('Harvest job does not exist: %s' % id)
        channel.basic_ack(method.delivery_tag)
        return False

    # Send the harvest job to the plugins that implement
    # the Harvester interface, only if the source type
    # matches
    harvester = get_harvester(job.source.type)

    if harvester:
        try:
            harvest_object_ids = gather_stage(harvester, job)
        except (Exception, KeyboardInterrupt):
            channel.basic_ack(method.delivery_tag)
            raise

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

    else:
        # This can occur if you:
        # * remove a harvester and it still has sources that are then refreshed
        # * add a new harvester and restart CKAN but not the gather queue.
        msg = 'System error - No harvester could be found for source type %s' % job.source.type
        err = HarvestGatherError(message=msg,job=job)
        err.save()
        log.error(msg)

    model.Session.remove()
    publisher.close()
    channel.basic_ack(method.delivery_tag)


def get_harvester(harvest_source_type):
    for harvester in PluginImplementations(IHarvester):
        if harvester.info()['name'] == harvest_source_type:
            return harvester


def gather_stage(harvester, job):
    '''Calls the harvester's gather_stage, returning harvest object ids, with
    some error handling.

    This is split off from gather_callback so that tests can call it without
    dealing with queue stuff.
    '''
    job.gather_started = datetime.datetime.utcnow()

    try:
        harvest_object_ids = harvester.gather_stage(job)
    except (Exception, KeyboardInterrupt):
        harvest_objects = model.Session.query(HarvestObject).filter_by(
            harvest_job_id=job.id
        )
        for harvest_object in harvest_objects:
            model.Session.delete(harvest_object)
        model.Session.commit()
        raise
    finally:
        job.gather_finished = datetime.datetime.utcnow()
        job.save()
    return harvest_object_ids


def fetch_callback(channel, method, header, body):
    try:
        id = json.loads(body)['harvest_object_id']
        log.info('Received harvest object id: %s' % id)
    except KeyError:
        log.error('No harvest object id received')
        channel.basic_ack(method.delivery_tag)
        return False

    try:
        obj = HarvestObject.get(id)
    except sqlalchemy.exc.OperationalError, e:
        # Occasionally we see: sqlalchemy.exc.OperationalError
        # "SSL connection has been closed unexpectedly"
        log.exception(e)
        log.error('Connection Error during gather of harvest object %s: %r %r',
                  id, e, e.args)
        # By not sending the ack, it will be retried later.
        # Try to clear the issue with a remove.
        model.Session.remove()
        return
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
    obj.fetch_started = datetime.datetime.utcnow()
    obj.state = "FETCH"
    obj.save()
    success_fetch = harvester.fetch_stage(obj)
    obj.fetch_finished = datetime.datetime.utcnow()
    obj.save()
    if success_fetch:
        # If no errors where found, call the import method
        obj.import_started = datetime.datetime.utcnow()
        obj.state = "IMPORT"
        obj.save()
        success_import = harvester.import_stage(obj)
        obj.import_finished = datetime.datetime.utcnow()
        if success_import:
            obj.state = "COMPLETE"
            if success_import is 'unchanged':
                obj.report_status = 'not modified'
                obj.save()
                return
        else:
            obj.state = "ERROR"
        obj.save()
    else:
        obj.state = "ERROR"
        obj.save()
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
    gather_routing_key = get_gather_routing_key()
    consumer = get_consumer(get_gather_queue_name(), gather_routing_key)
    log.debug('Gather queue consumer registered')
    return consumer


def get_fetch_consumer():
    fetch_routing_key = get_fetch_routing_key()
    consumer = get_consumer(get_fetch_queue_name(), fetch_routing_key)
    log.debug('Fetch queue consumer registered')
    return consumer


def get_gather_publisher():
    gather_routing_key = get_gather_routing_key()
    return get_publisher(gather_routing_key)


def get_fetch_publisher():
    fetch_routing_key = get_fetch_routing_key()
    return get_publisher(fetch_routing_key)

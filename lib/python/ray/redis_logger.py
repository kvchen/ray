import logging
import redis
import datetime

import config


# 'entity_type', 'entity_id', 'related_entity_ids', and 'event_type' can be specified in a dictionary
# passed into Python logging calls under the kwarg 'extra'.
# Ex: logger.info("Logging message", extra={'entity_type': 'TASK', 'related_entity_ids': [3, 5, 6]})
REDIS_RECORD_FIELDS = ['log_level',
                       'entity_type',
                       'entity_id',
                       'related_entity_ids',
                       'event_type',
                       'origin',
                       'message']

RAY_FUNCTION = 'FUNCTION'
RAY_OBJECT = 'OBJECT'
RAY_TASK = 'TASK'

class RedisHandler(logging.Handler):
  def __init__(self, origin_type, address, redis_host='localhost', redis_port='6379'):
    logging.Handler.__init__(self)
    self.origin = "{origin_type}:{address}".format(origin_type=origin_type,
                                                   address=address)
    self.redis = redis.StrictRedis(host=redis_host,
                                   port=redis_port)
    self.table = 'log'

  def emit(self, record):
    # Key is <tablename>:<timestamp>.
    timestamp = datetime.datetime.now()
    key = "{table}:{timestamp}".format(table=self.table, timestamp=timestamp)
    record_dict = {}
    for field in REDIS_RECORD_FIELDS:
      record_dict[field] = getattr(record, field, '')
    related_entity_ids = [str(entity_id) for entity_id in getattr(record, 'related_entity_ids', [])]
    record_dict['related_entity_ids'] = ' '.join(related_entity_ids)
    record_dict['origin'] = self.origin
    record_dict['log_level'] = logging.getLevelName(self.level)
    self.redis.hmset(key, record_dict)

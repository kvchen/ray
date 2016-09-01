import logging
import redis
import datetime

import config


# 'entity_type', 'entity_id', and 'event_type' can be specified in a dictionary
# passed into Python logging calls under the kwarg 'extra'.
# Ex: logger.info("Logging message", extra={'entity_type': 'TASK'})
REDIS_RECORD_FIELDS = ['log_level',
                       'entity_type',
                       'entity_id',
                       'event_type',
                       'origin',
                       'message']

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
    record_dict['origin'] = self.origin
    record_dict['log_level'] = logging.getLevelName(self.level)
    self.redis.hmset(key, record_dict)

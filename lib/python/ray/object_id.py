# TODO(rkn): Get rid of this.
import random
def random_object_id():
  return ObjectID("".join([chr(random.randint(0, 255)) for _ in range(20)]))

class ObjectID(object):
  def __init__(self, object_id):
    self.object_id = object_id

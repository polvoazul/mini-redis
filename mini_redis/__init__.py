import redis
import json
import msgpack

class MiniRedis(redis.StrictRedis):

	def save_json(self, key, obj):
		self.set(key, msgpack.dumps(obj))

	def load_json(self, key):
		return msgpack.loads(self.get(key), encoding='utf8')

import redis
import json
import msgpack

SCHEMAS_KEY = '__mini-redis:schemas'
class MiniRedis(redis.StrictRedis):
	def save_json(self, key, obj):
		self.set(key, msgpack.dumps(obj))

	def load_json(self, key):
		return msgpack.loads(self.get(key), encoding='utf8')

	def save_json_fixed_schema(self, key, obj): # TODO: make a concurrent
		schema = get_schema(obj)
		index = self.zscore(SCHEMAS_KEY, schema)
		if index is None:
			index = self.zcard(SCHEMAS_KEY)
			self.zadd(SCHEMAS_KEY, index, schema)
		values = [int(index)] + [v for k, v in sorted(obj.items())]
		self.set(key, msgpack.dumps( values ) )

	def load_json_fixed_schema(self, key):
		index, *values = msgpack.loads(self.get(key), encoding='utf8')
		schema = json.loads(self.zrangebyscore(SCHEMAS_KEY, index, index)[0].decode('utf8'))
		return apply_schema_on_values(schema, values)

def apply_schema_on_values(schema, values):
	return dict(zip(schema, values))

def get_schema(obj):
	# support top level keys schema only for now
	return json.dumps(sorted(obj.keys()))
	# return json.dumps({k: None for k in obj.keys()}, sort_keys=True)

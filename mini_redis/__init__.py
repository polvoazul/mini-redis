import redis
import json
import msgpack
import functools

SCHEMAS_KEY = '__mini-redis:schemas'
class MiniRedis():
	def __new__(cls):
		return MiniRedis.SchemaFactoring()

	class Base(redis.StrictRedis):
		def get_memory(self):
			return sum(self.execute_command('memory usage {}'.format(k.decode('utf8'))) for k in self.keys())

	class SimpleMsgpack(Base):
		def save_json(self, key, obj):
			self.set(key, msgpack.dumps(obj))

		def load_json(self, key):
			return msgpack.loads(self.get(key), encoding='utf8')

	class SchemaFactoring(Base):
		@functools.lru_cache()
		def get_schema_index(self, SCHEMAS_KEY, schema):
			index = self.zscore(SCHEMAS_KEY, schema)
			if index is None:
				index = self.zcard(SCHEMAS_KEY)
				self.zadd(SCHEMAS_KEY, index, schema)
			return index

		def save_json(self, key, obj): # TODO: make it concurrent
			schema = get_schema(obj)
			index = self.get_schema_index(SCHEMAS_KEY, schema)
			values = [int(index)] + [v for k, v in sorted(obj.items())]
			self.set(key, msgpack.dumps( values ) )

		def load_json(self, key):
			index, *values = msgpack.loads(self.get(key), encoding='utf8')
			schema = msgpack.loads(self.zrangebyscore(SCHEMAS_KEY, index, index)[0], encoding='utf8')
			return apply_schema_on_values(schema, values)


def apply_schema_on_values(schema, values):
	return dict(zip(schema, values))

def get_schema(obj):
	# support top level keys schema only for now
	return msgpack.dumps(sorted(obj.keys()))

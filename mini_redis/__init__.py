import redis
import json
import msgpack
import functools

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
		SCHEMAS_KEY = '__mini-redis:schemas'

		@functools.lru_cache()
		def get_schema(self, index):
			return msgpack.loads(self.zrangebyscore(self.SCHEMAS_KEY, index, index)[0], encoding='utf8')

		@functools.lru_cache()
		def get_schema_index(self, schema):
			index = self.zscore(self.SCHEMAS_KEY, schema)
			if index is None:
				index = self.store_schema(schema)
			return index

		def store_schema(self, schema):
			index = self.zcard(self.SCHEMAS_KEY)
			self.zadd(self.SCHEMAS_KEY, index, schema)
			return index

		def save_json(self, key, obj): # TODO: make it concurrent
			schema = get_schema(obj)
			if False: # self.redis == True: #'pipeline':
				index = self.get_schema_index(schema)
				values = [int(index)] + [v for k, v in sorted(obj.items())]
				self.set(key, msgpack.dumps( values ) )
			else:
				self.eval('''
						local idx = redis.call('zscore', KEYS[1], ARGV[1])
						if not idx then
							idx = redis.call('zcard', KEYS[1])
							redis.call('zadd', KEYS[1], idx, ARGV[1])
						end
						local t = cjson.decode(ARGV[2])
						table.insert(t, 1, idx)
						redis.call('set', KEYS[2], cmsgpack.pack(t))
				''', 2, self.SCHEMAS_KEY, key, schema, json.dumps([v for k, v in sorted(obj.items())]))

		def load_json(self, key):
			index, *values = msgpack.loads(self.get(key), encoding='utf8')
			schema = self.get_schema(index)
			return apply_schema_on_values(schema, values)

def apply_schema_on_values(schema, values):
	return dict(zip(schema, values))

def get_schema(obj):
	# support top level keys schema only for now
	return msgpack.dumps(sorted(obj.keys()))

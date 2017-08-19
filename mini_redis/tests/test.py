import pytest
import mini_redis

class TestMiniRedis():
	def setup(self):
		self.redis = mini_redis.MiniRedis()
		self.redis.flushall()

	def test_json(self):
		redis = self.redis
		OBJ = {'2':3, 'pedra': 'caminho'}
		key = "KEY"

		redis.save_json(key, OBJ)
		assert redis.load_json(key) == OBJ
		memory = _get_memory(redis)

		self.redis.flushall()
		redis.hmset(key, OBJ)
		baseline_memory = _get_memory(redis)

		assert memory < baseline_memory
		print('Memory: {}% size of baseline'.format(100.0*memory/baseline_memory))

	def test_many_jsons(self):
		pass

def _get_memory(redis):
	return sum(redis.execute_command('memory usage {}'.format(k.decode('utf8'))) for k in redis.keys())

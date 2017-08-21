import pytest
import mini_redis

from faker import Faker
faker = Faker()

class TestMiniRedis():
	def setup(self):
		self.redis = mini_redis.MiniRedis()
		self.redis.flushall()

	def test_json(self):
		redis = self.redis
		OBJ = {'age':31, 'height': 168, 'name': 'Carol', 'favorite_color': 'magenta', 'interests':['gardening', 'rock climbing', 'biology']}
		key = "KEY"

		redis.save_json(key, OBJ)
		assert redis.load_json(key) == OBJ
		memory = _get_memory(redis)

		self.redis.flushall()
		redis.hmset(key, OBJ)
		baseline_memory = _get_memory(redis)

		assert memory < baseline_memory
		print('Memory: {:.4}% size of baseline'.format(100.0*memory/baseline_memory))

	def test_many_same_structured_jsons(self):
		redis = self.redis

		OBJS = [ ('KEY' + str(i),
				{'age': faker.pyint() % 99, 'height': faker.pyint() % 100 + 100, 'name': faker.name(),
				'favorite_color': faker.color_name(), 'interests':[faker.slug(),
				faker.slug(), faker.slug()]})
			for i in range(1,1000)]

		for key, obj in OBJS:
			redis.save_json_fixed_schema(key, obj)
			assert redis.load_json_fixed_schema(key) == obj
		memory = _get_memory(redis)

		self.redis.flushall()

		for key, obj in OBJS:
			redis.hmset(key, obj)
		baseline_memory = _get_memory(redis)

		assert memory < baseline_memory
		print('Memory: {:.4}% size of baseline'.format(100.0*memory/baseline_memory))

def _get_memory(redis):
	return sum(redis.execute_command('memory usage {}'.format(k.decode('utf8'))) for k in redis.keys())

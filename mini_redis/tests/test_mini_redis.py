import pytest
import mini_redis
import redis

from faker import Faker
faker = Faker()

class TestMiniRedis():
	def setup(self):
		self.redis = redis.StrictRedis()
		self.redis.flushall()

	def test_single_json(self):
		redis = mini_redis.MiniRedis.SimpleMsgpack()
		OBJ = {'age':31, 'height': 168, 'name': 'Carol', 'favorite_color': 'magenta', 'interests':['gardening', 'rock climbing', 'biology']}
		key = "KEY"

		redis.save_json(key, OBJ)
		assert redis.load_json(key) == OBJ
		memory = redis.get_memory()

		self.redis.flushall()
		redis.hmset(key, OBJ)
		baseline_memory = redis.get_memory()

		assert memory < baseline_memory
		print('Memory: {:.4}% size of baseline'.format(100.0*memory/baseline_memory))

	def test_many_same_structured_jsons(self):
		redis = mini_redis.MiniRedis.SchemaFactoring()

		OBJS = [ ('KEY' + str(i),
				{'age': faker.pyint() % 99, 'height': faker.pyint() % 100 + 100, 'name': faker.name(),
				'favorite_color': faker.color_name(), 'interests':[faker.slug(),
				faker.slug(), faker.slug()]})
			for i in range(1,1000)]

		for key, obj in OBJS:
			redis.save_json(key, obj)
			assert redis.load_json(key) == obj
		memory = redis.get_memory()

		self.redis.flushall()

		for key, obj in OBJS:
			redis.hmset(key, obj)
		baseline_memory = redis.get_memory()

		assert memory < baseline_memory
		print('Memory: {:.4}% size of baseline'.format(100.0*memory/baseline_memory))


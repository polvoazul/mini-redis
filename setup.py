from setuptools import setup, find_packages

setup(name='mini_redis',
		version='0.1.1',
		description='A lib to store structured data in redis using less memory',
		url='http://github.com/polvoazul/mini-redis',
		author='Fred Israel',
		#author_email='',
		license='unlicense',
		packages=find_packages(),
		zip_safe=False,
		python_requires='>=3.3, <4',
		install_requires=[
				'redis',
				'msgpack-python'],
		tests_require=[
				'pytest',
				'faker'
				]
)

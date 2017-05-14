import os
import subprocess
from contextlib import contextmanager
import datetime
import itertools
from peewee import SqliteDatabase, Model, IntegerField, BooleanField, DoubleField


os.chdir(os.path.dirname(os.path.realpath(__file__)))
os.chdir('..')

db = SqliteDatabase('timing.db')


class Timing(Model):
	block_size = IntegerField()
	compression = BooleanField()
	readahead = BooleanField()
	item_type = IntegerField()
	test = IntegerField()
	duration = DoubleField()
	timestamp = IntegerField()

	class Meta:
		database = db


MB = 2**20
min_bs = MB // 16
max_bs = 4 * MB

blocksizes = [min_bs]
while blocksizes[-1] < max_bs:
	blocksizes.append(blocksizes[-1] * 2)


items = 1
tests = 2


@contextmanager
def chdir(path):
	cwd = os.getcwd()
	os.chdir(path)
	try:
		yield
	finally:
		os.chdir(cwd)


def build(bs):
	path = 'build-speed-test/bs-' + str(bs)
	subprocess.check_call(['mkdir', '-p', path])
	with chdir(path):
		subprocess.check_call(['cmake', '-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_CXX_FLAGS=-DFILE_STREAM_BLOCK_SIZE=' + str(bs), '../..'])
		subprocess.check_call(['make', '-j2'])


def buildall():
	for bs in blocksizes:
		build(bs)


def kill_cache():
	subprocess.check_call(['./killcache'])


now = datetime.datetime.utcnow


def run_test(bs, compression=True, readahead=True, item=0, test=0):
	start = now()
	path = 'build-speed-test/bs-' + str(bs)
	with chdir(path):
		subprocess.check_call(['./speed_test'] + [str(int(v)) for v in [compression, readahead, item, test]])
	end = now()
	return (end - start).total_seconds()


def runall():
	bins = [False, True]
	for args in itertools.product(blocksizes, bins, bins, range(items), range(tests)):
		kill_cache()
		time = run_test(*args)

		Timing.create(
			block_size=args[0],
			compression=args[1],
			readahead=args[2],
			item_type=args[3],
			test=args[4],
			duration=time,
			timestamp=int(now().timestamp()),
		)


if __name__ == '__main__':
	db.connect()
	db.create_table(Timing, safe=True)

	buildall()

	runall()

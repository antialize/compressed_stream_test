import sys
import os
import signal
from subprocess import run, check_call, Popen, PIPE
from contextlib import contextmanager
import datetime
import itertools
from peewee import SqliteDatabase, Model, IntegerField, BooleanField, DoubleField
import progressbar


os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')

db = SqliteDatabase('timing.db')


class Timing(Model):
	block_size = IntegerField()
	compression = BooleanField()
	readahead = BooleanField()
	item_type = IntegerField()
	test = IntegerField()
	parameter = IntegerField()
	duration = DoubleField()
	timestamp = IntegerField()

	class Meta:
		database = db


def exprange(start, stop):
	val = start
	while val != stop:
		yield val
		val *= 2

	yield val


MB = 2**20
min_bs = MB // 16
#max_bs = 4 * MB
max_bs = min_bs

blocksizes = list(exprange(min_bs, max_bs))

items = 3
tests = 8

def parameters(test):
	# Merge tests
	if test in [4, 5]: 
		return exprange(2, 512)
	else:
		return [0]


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
	check_call(['mkdir', '-p', path])
	with chdir(path):
		check_call(['cmake', '-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_CXX_FLAGS=-DFILE_STREAM_BLOCK_SIZE=' + str(bs), '../..'])
		check_call(['make', '-j8', 'speed_test'])


def buildall():
	for bs in blocksizes:
		build(bs)


def kill_cache():
	p = run(['killcache'])
	if p.returncode not in [0, -signal.SIGKILL]:
		print('killcache failed', file=sys.stderr)
		sys.exit(1)


now = datetime.datetime.utcnow


def run_test(bs, compression, readahead, item, test, parameter):
	start = now()
	path = 'build-speed-test/bs-' + str(bs)
	with chdir(path):
		for setup in [True, False]:
			if not setup:
				kill_cache()
			p = Popen(['./speed_test'] + [str(int(v)) for v in [compression, readahead, item, test, setup, parameter]], stdout=PIPE, stderr=PIPE)
			stdout, stderr = p.communicate()
			if stderr.endswith(b'SKIP\n'):
				return None
	end = now()
	return (end - start).total_seconds()


def runall():
	bins = [False, True]

	arg_combinations = []
	
	for args in itertools.product(blocksizes, bins, bins, range(items), range(tests)):
		for parameter in parameters(args[-1]):
			arg_combinations.append(args + (parameter,))
	
	bar = progressbar.ProgressBar()

	for args in bar(arg_combinations):
		time = run_test(*args)

		if time == None:
			print('Skipped', *args)
			continue

		Timing.create(
			block_size=args[0],
			compression=args[1],
			readahead=args[2],
			item_type=args[3],
			test=args[4],
			parameter=args[5],
			duration=time,
			timestamp=int(now().timestamp()),
		)


if __name__ == '__main__':
	db.connect()
	db.create_table(Timing, safe=True)

	buildall()

	runall()

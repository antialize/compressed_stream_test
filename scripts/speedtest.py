import sys
import os
import signal
from subprocess import run, check_call, Popen, PIPE, DEVNULL
from contextlib import contextmanager
import datetime
import time
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


bins = [False, True]

items = 3
tests = 8

MB = 2**20
min_bs = MB // 16
#max_bs = 4 * MB
max_bs = min_bs

blocksizes = list(exprange(min_bs, max_bs))
compression_args = bins
readahead_args = bins
item_args = range(items)
test_args = range(tests)


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
		check_call(['cmake', '-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_CXX_FLAGS=-march=native -DFILE_STREAM_BLOCK_SIZE=' + str(bs), '../..'])
		check_call(['make', '-j8', 'speed_test'])


def buildall():
	for bs in blocksizes:
		build(bs)


def format_partition():
    mountpoint = '/hdd'
    device = '/dev/sdb1'

    p = run(['mountpoint', '-q', mountpoint])
    if p.returncode == 0:
        check_call(['sudo', 'umount', device])

    check_call(['sudo', 'mkfs.ext4', '-F', device], stdout=DEVNULL, stderr=DEVNULL)
    check_call(['sudo', 'mount', device, mountpoint])
    check_call(['sudo', 'mkdir', mountpoint + '/tmp'])
    check_call(['sudo', 'chown', 'madalgo:madalgo', mountpoint + '/tmp'])


def kill_cache():
	p = run(['killcache'])
	if p.returncode not in [0, -signal.SIGKILL]:
		print('killcache failed', file=sys.stderr)
		sys.exit(1)


now = lambda: time.clock_gettime(time.CLOCK_MONOTONIC_RAW)


def run_test(bs, compression, readahead, item, test, parameter):
	format_partition()
	path = 'build-speed-test/bs-' + str(bs)
	with chdir(path):
		for setup in [True, False]:
			if not setup:
				kill_cache()
				start = now()
			p = Popen(['./speed_test'] + [str(int(v)) for v in [compression, readahead, item, test, setup, parameter]], stdout=PIPE, stderr=PIPE)
			stdout, stderr = p.communicate()
			if not setup:
				end = now()
			if stderr.endswith(b'SKIP\n'):
				return None
	return end - start


def runall():
	arg_combinations = []

	for args in itertools.product(blocksizes, compression_args, readahead_args, item_args, test_args):
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
			timestamp=int(datetime.datetime.utcnow().timestamp()),
		)


if __name__ == '__main__':
	db.connect()
	db.create_table(Timing, safe=True)

	buildall()

	runall()

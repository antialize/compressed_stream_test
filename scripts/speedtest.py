import sys
import os
import signal
from subprocess import run, check_call, PIPE, DEVNULL
from contextlib import contextmanager
import datetime
import time
import itertools
from peewee import SqliteDatabase, Model, IntegerField, BooleanField, DoubleField
import progressbar


DIRS = ['tpie', 'compressed_stream_test']

if len(sys.argv) >= 2:
	config_filename = os.path.abspath(sys.argv[1])
else:
	config_filename = None

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../..')

# project = os.path.basename(os.getcwd())
# USING_TPIE = project == 'tpie'

dt = datetime.datetime.now()
db = SqliteDatabase('timing_%s.db' % dt.isoformat())

class Timing(Model):
	old_streams = BooleanField()
	block_size = IntegerField()
	file_size = IntegerField()
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

DEBUG = True
SHOULD_KILLCACHE = False
SHOULD_FORMAT = False
SHOULD_VALIDATE = True
action_args = range(3) if SHOULD_VALIDATE else range(2)

MB = 2**20
min_bs = MB // 16
max_bs = 4 * MB

# In megabytes
min_fs = 1024
max_fs = 128 * 1024

blocksizes = list(exprange(min_bs, max_bs))
filesizes = list(exprange(min_fs, max_fs))
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


def build_path(d, bs, fs):
	return '%s/build-speed-test/bs-%s-%s' % (d, bs, fs)


def build(d, bs, fs):
	path = build_path(d, bs, fs)
	check_call(['mkdir', '-p', path])
	with chdir(path):
		build_type = 'Debug' if DEBUG else 'Release'

		check_call(['cmake', '-DCMAKE_BUILD_TYPE=' + build_type, '-DCMAKE_CXX_FLAGS=-march=native -DFILE_STREAM_BLOCK_SIZE=' + str(bs) + ' -DSPEED_TEST_FILE_SIZE_MB=' + str(fs), '../..'])
		check_call(['make', '-j1', 'speed_test'])


def buildall():
	for d in DIRS:
		for bs in blocksizes:
			for fs in filesizes:
				build(d, bs, fs)


def format_partition():
	if not SHOULD_FORMAT:
		return

	mountpoint = '/hdd'
	device = '/dev/sdb1'

	p = run(['mountpoint', '-q', mountpoint])
	if p.returncode == 0:
		check_call(['sudo', 'umount', device])

	check_call(['sudo', 'mkfs.ext4', '-F', device], stdout=DEVNULL, stderr=DEVNULL)
	check_call(['sudo', 'mount', device, mountpoint])
	check_call(['sudo', 'mkdir', mountpoint + '/tmp'])
	check_call(['sudo', 'chown', '%s:%s' % tuple([os.getlogin()] * 2), mountpoint + '/tmp'])


def kill_cache():
	if not SHOULD_KILLCACHE:
		return

	p = run(['killcache'])
	if p.returncode not in [0, -signal.SIGKILL]:
		print('killcache failed', file=sys.stderr)
		sys.exit(1)


now = lambda: time.clock_gettime(time.CLOCK_MONOTONIC_RAW)


def run_test(bs, fs, compression, readahead, item, test, parameter, old_streams):
	format_partition()
	path = build_path(DIRS[old_streams], bs, fs)
	with chdir(path):
		for action in action_args:
			if action == 1:
				kill_cache()
				start = now()
			args = [str(int(v)) for v in [compression, readahead, item, test, action, parameter]]
			all_args = ['./speed_test'] + args
			print('Running', path, *all_args)
			p = run(all_args, stdout=PIPE, stderr=PIPE)
			if p.returncode != 0:
				print('\nFailed to run speed_test with arguments: %s' % args, file=sys.stderr)
				print('Exit code:', p.returncode)
				print(str(p.stderr, 'utf-8'), file=sys.stderr)
				print(str(p.stdout, 'utf-8'), file=sys.stderr)
				sys.exit(1)
			if action == 1:
				end = now()
			if p.stderr.endswith(b'SKIP\n'):
				return None

	return end - start


def get_arg_combinations():
	arg_combinations = []

	for args in itertools.product(blocksizes, filesizes, compression_args, readahead_args, item_args, test_args):
		for parameter in parameters(args[-1]):
			# Add old_streams as last argument
			for old_streams in bins:
				arg_combinations.append(args + (parameter, old_streams))

	return arg_combinations


def runall():
	arg_combinations = get_arg_combinations()

	bar = progressbar.ProgressBar(redirect_stdout=True)

	for args in bar(arg_combinations):
		time = run_test(*args)

		if time == None:
			print('Skipped', *args)
			continue

		Timing.create(
			old_streams=args[7],
			block_size=args[0],
			file_size=args[1],
			compression=args[2],
			readahead=args[3],
			item_type=args[4],
			test=args[5],
			parameter=args[6],
			duration=time,
			timestamp=int(datetime.datetime.utcnow().timestamp()),
		)


if __name__ == '__main__':
	if config_filename:
		print('Loading config from %s' % sys.argv[1])
		with open(config_filename) as f:
			exec(f.read())
	else:
		print('Using default config')

	print('Test matrix size: %s' % len(get_arg_combinations()))

	db.connect()
	db.create_table(Timing, safe=True)

	buildall()

	runall()

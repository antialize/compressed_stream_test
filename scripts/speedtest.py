import os
import subprocess
from contextlib import contextmanager
import datetime
import json
import itertools

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


def runall(data):
	bins = [False, True]
	for args in itertools.product(blocksizes, bins, bins, range(items), range(tests)):
		kill_cache()
		time = run_test(*args)

		key = ','.join(str(int(v)) for v in args)
		if key not in data:
			data[key] = []

		data[key].append({'timing': time, 'timestamp': int(now().timestamp())})
		with open('timing.json', 'w') as f:
			json.dump(data, f)


if __name__ == '__main__':
	os.chdir(os.path.dirname(os.path.realpath(__file__)))
	os.chdir('..')
	buildall()

	try:
		with open('timing.json', 'r') as f:
			data = json.load(f)
	except:
		data = {}
		with open('timing.json', 'w') as f:
			json.dump(data, f)

	runall(data)

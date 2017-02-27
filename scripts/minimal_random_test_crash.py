#!/usr/bin/env python3
import sys
import subprocess
import progressbar

TIMEOUT = 2

def args_for_tasks(tasks, threads, max_streams, seed='0'):
	return [random_test, '-t', threads, '-s', max_streams, '-r', seed, '-w', *tasks]

def crashes(tasks, threads, max_streams):
	try:
		subprocess.run(args_for_tasks(tasks, threads=threads, max_streams=max_streams), check=True, timeout=TIMEOUT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
	except subprocess.CalledProcessError:
		return True
	except subprocess.TimeoutExpired:
		return False

	assert False

#def get_unneeded_tasks(tasks_to_check, tasks):
#	m = len(tasks_to_check) // 2
#	t1, t2 = tasks_to_check[:m], tasks_to_check[m:]
#
#	ret = []
#
#	for t in (t1, t2):
#		if t:
#			if crashes(set(tasks) - set(t)):
#				# If we crash without t, we don't need those
#				ret += t
#			else:
#				# If we don't crash, we need to find out which part of t crashes
#				if len(t) == 1:
#					# If there is only one element we know that t[0] is needed to crash
#					pass
#				else:
#					ret += get_unneeded_tasks(t, tasks)
#	
#	if ret:
#		print('Not needed:', ret)
#	return ret

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('Usage: %s [random_test binary]' % sys.argv[0])
		sys.exit(1)

	random_test = sys.argv[1]

	help_string = str(subprocess.check_output([random_test, '-h']), 'utf-8')
	_, tasks_string = help_string.split('Task names:')
	tasks = tasks_string.strip().split()

	print('Determining how many threads and max streams are needed...')

	for threads, max_streams in ('11', '12', '21', '22'):
		if crashes(tasks, threads=threads, max_streams=max_streams):
			break
	else:
		print("random_test doesn't seem to crash with all tasks enabled.")
		sys.exit()

	print('Running random_test with %s threads and %s max streams' % (threads, max_streams))

	print('Finding minimal set of tasks that causes random_test to crash...')
	
	with progressbar.ProgressBar(max_value=len(tasks)) as bar:
		enabled_tasks = tasks
		i = 0
		while i < len(enabled_tasks):
			t = enabled_tasks[i]
			s = enabled_tasks[:]
			s.remove(t)
			if crashes(s, threads=threads, max_streams=max_streams):
				enabled_tasks = s
			else:
				i += 1
			bar.update(bar.value + 1)
	
	print('Minimal set of tasks that crashes random_test:')
	print(*enabled_tasks)
	print()

	print('Finding seed that runs fewest total tasks before crashing...')

	best = (float('inf'), 0)
	bar = progressbar.ProgressBar()
	for i in bar(range(100)):
		try:
			p = subprocess.run(args_for_tasks(enabled_tasks, threads=threads, max_streams=max_streams, seed=str(i)), timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
		except subprocess.TimeoutExpired:
			continue

		assert p.returncode != 0
		val = len(p.stdout.split(b'==>')) - 1

		if val < best[0]:
			best = (val, i)
	
	print('Running with seed %s executed %s tasks before crashing' % (best[1], best[0]))
	print()

	print('Command-line to run random_test:')
	print(*args_for_tasks(enabled_tasks, threads=threads, max_streams=max_streams, seed=best[1]))


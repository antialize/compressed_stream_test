import argparse
import re
from collections import defaultdict
from humanize import naturalsize


STRACE_RE = re.compile(
r'''
(?P<pid>\d+)
\s+
(?P<timestamp>\d+\.\d+)
\s+
(?P<syscall>\w+)
\((?P<args>[^)]*)\)
\s+
=
\s+
(?P<ret>\d+)
\s+
<(?P<time>\d+\.\d+)>
''', re.VERBOSE)


class Syscall:
	__slots__ = ['calls', 'timings', 'timestamps']
	def __init__(self):
		self.calls = 0
		self.timings = []
		self.timestamps = []

	def total_time(self):
		return sum(self.timings)


class IOSyscallGroup(Syscall):
	__slots__ = ['bytes']
	def __init__(self):
		super().__init__()
		self.bytes = 0


parser = argparse.ArgumentParser()
parser.add_argument('strace_log', type=argparse.FileType('r', encoding='UTF-8'))
parser.add_argument('--plot', action='store_true')

args = parser.parse_args()


start_time = None
end_time = None
total_time = 0

read = IOSyscallGroup()
write = IOSyscallGroup()

groups = {
	'read': read,
	'pread64': read,
	'write': write,
	'pwrite64': write,
}

syscalls = defaultdict(Syscall)

for l in args.strace_log:
	m = STRACE_RE.match(l)
	if not m:
		# print(l)
		continue

	d = m.groupdict()
	syscall = d['syscall']
	ret = int(d['ret'])
	time = float(d['time'])
	timestamp = float(d['timestamp'])

	if start_time == None:
		start_time = timestamp
	end_time = timestamp

	s = syscalls[syscall]
	s.calls += 1
	s.timings.append(time)
	s.timestamps.append(timestamp)

	g = groups.get(syscall)
	if g:
		g.calls += 1
		g.bytes += ret
		g.timings.append(time)
		g.timestamps.append(timestamp)

	total_time += time


print('Traced syscalls:')
for name, s in syscalls.items():
	print(f'  {name} ({round(s.total_time(), 7)} s) ({s.calls} calls)')
print()
print(f'Total time in traced syscalls: {total_time} s')
print(f'Total time: {end_time - start_time} s')
print()
print(f'Read:    {naturalsize(read.bytes) } ({round(read.total_time(),  7)} s) ({read.calls } calls)')
print(f'Written: {naturalsize(write.bytes)} ({round(write.total_time(), 7)} s) ({write.calls} calls)')


if args.plot:
	import matplotlib.pyplot as plt
	import matplotlib.collections as mc

	fig, ax = plt.subplots()
	for g, color in [(read, 'blue'), (write, 'red')]:
		lines = []
		for y, (x, dx) in enumerate(zip(g.timestamps, g.timings)):
			x -= start_time
			lines.append([(x, y), (x + dx, y + 1)])

		ax.add_collection(mc.LineCollection(lines, linewidths=1, color=color))

	ax.autoscale()
	plt.show(fig)

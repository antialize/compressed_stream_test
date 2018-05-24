import argparse
import re
from collections import defaultdict
from humanize import naturalsize

STRACE_RE_START = r'''
(?P<pid>\d+)
\s+
(?P<timestamp>\d+\.\d+)
\s+
'''

STRACE_RE_ARGS = r'''
(?P<args>(?:[^}]*})?[^)]*)
'''

STRACE_RE_MIDDLE = r'''
(?P<syscall>\w+)
\(
''' + STRACE_RE_ARGS


STRACE_RE_END = r'''
\)
\s+
=
\s+
(?P<ret>-?(?:0x)?[0-9a-f]+|\?)
(?:\s+[^<]+)? # Error message
\s+
<(?P<time>\d+\.\d+)>
'''

STRACE_RE = re.compile(STRACE_RE_START + STRACE_RE_MIDDLE + STRACE_RE_END, re.VERBOSE)

STRACE_UNFINISHED_RE = re.compile(STRACE_RE_START + STRACE_RE_MIDDLE + r'\s+<unfinished\s+\.\.\.>', re.VERBOSE)
STRACE_RESUMED_RE = re.compile(STRACE_RE_START + r'<\.\.\.\s+(?P<syscall>\w+)\s+resumed>\s+' + STRACE_RE_ARGS + STRACE_RE_END, re.VERBOSE)


class Syscall:
	__slots__ = ['name', 'pid', 'timestamp', 'ret', 'time']
	def __init__(self, d):
		self.name = d['syscall']
		self.pid = int(d['pid'])
		self.timestamp = float(d['timestamp'])
		self.time = float(d['time'])

		ret = d['ret']

		if ret == '?':
			self.ret = None
		elif ret.startswith('0') and not ret.startswith('0x'):
			self.ret = int(ret, 8)
		else:
			self.ret = int(ret, 0)

class SyscallStats:
	__slots__ = ['calls', 'total_time']
	def __init__(self):
		self.calls = 0
		self.total_time = 0

class IOSyscallStats(SyscallStats):
	__slots__ = ['total_bytes', 'name']
	def __init__(self, name):
		super().__init__()
		self.total_bytes = 0
		self.name = name

parser = argparse.ArgumentParser()
parser.add_argument('strace_log', type=argparse.FileType('r', encoding='UTF-8'))
parser.add_argument('--plot', action='store_true')

args = parser.parse_args()

start_time = None
end_time = None
total_time = 0

read_stats = IOSyscallStats('read')
write_stats = IOSyscallStats('write')

stat_groups = {
	'read': read_stats,
	'pread64': read_stats,
	'write': write_stats,
	'pwrite64': write_stats,
}

pids = set()
syscalls = []
syscall_stats = defaultdict(SyscallStats)
unfinished_syscalls = {}

for l in args.strace_log:
	m = STRACE_RE.match(l)
	if m:
		d = m.groupdict()
	else:
		m = STRACE_UNFINISHED_RE.match(l)
		if m:
			pid = int(m.group('pid'))
			pids.add(pid)
			assert pid not in unfinished_syscalls
			unfinished_syscalls[pid] = m.groupdict()
			continue
		else:
			m = STRACE_RESUMED_RE.match(l)
			if m:
				pid = int(m.group('pid'))
				assert pid in unfinished_syscalls
				d1 = unfinished_syscalls.pop(pid)
				d2 = m.groupdict()
				assert d1['syscall'] == d2['syscall']

				diff = float(d2['timestamp']) - float(d1['timestamp'])
				assert diff >= 0

				d = d1
				d['ret'] = d2['ret']
				d['time'] = str(float(d2['time']) + diff)
				d['args'] += d2['args']
			else:
				print('Couldn\'t parse line:', l, end='')
				continue

	syscall = Syscall(d)

	syscalls.append(syscall)
	pids.add(syscall.pid)

	stats = syscall_stats[syscall.name]
	stats.calls += 1
	stats.total_time += syscall.time

	if start_time == None:
		start_time = syscall.timestamp

	end_time = syscall.timestamp
	total_time += syscall.time

	group_stats = stat_groups.get(syscall.name)
	if group_stats:
		group_stats.calls += 1
		group_stats.total_time += syscall.time
		group_stats.total_bytes += syscall.ret


print(f'Pids: ({len(pids)})')
for pid in sorted(pids):
	print(f'  {pid}')
print()

print('Traced syscalls:')
for name, s in sorted(syscall_stats.items(), key=lambda x: -x[1].total_time):
	print(f'  {name} ({round(s.total_time, 7)} s) ({s.calls} calls)')
print()
print(f'Total time in traced syscalls: {total_time} s')
print(f'Total time: {end_time - start_time} s')
print()
print(f'Read:    {naturalsize(read_stats.total_bytes) } ({round(read_stats.total_time,  7)} s) ({read_stats.calls } calls)')
print(f'Written: {naturalsize(write_stats.total_bytes)} ({round(write_stats.total_time, 7)} s) ({write_stats.calls} calls)')


if args.plot:
	import matplotlib.pyplot as plt
	import matplotlib.collections as mc

	fig, ax = plt.subplots()

	lines = defaultdict(list)

	counter = defaultdict(int)

	for syscall in syscalls:
		if syscall.name not in stat_groups:
			continue

		color = 'blue' if stat_groups[syscall.name].name == 'read' else 'red'

		x = syscall.timestamp - start_time
		dx = syscall.time
		y = counter[syscall.name]
		dy = 1

		lines[color].append([(x, y), (x + dx, y + dy)])

		counter[syscall.name] += 1


	for color, ls in lines.items():
		ax.add_collection(mc.LineCollection(ls, linewidths=1, color=color))

	ax.autoscale()
	plt.show(fig)

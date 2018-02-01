import sys
import os
import shutil
from subprocess import check_call
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import LogFormatterSciNotation
from collections import defaultdict
from cycler import cycler
import argparse
import json

OUTPUT_DIR = 'plots'

os.chdir(str(Path(__file__).parent))

# See https://github.com/matplotlib/matplotlib/pull/9255
CB_color_cycle = list(map(lambda x: '#' + x,
['006BA4', 'FF800E', 'ABABAB', '595959', '5F9ED1', 'C85200', '898989', 'A2C8EC', 'FFBC79', 'CFCFCF']
))
matplotlib.rc('axes', prop_cycle=cycler(color=CB_color_cycle))

def savefig(fig, lgd, filename):
	print("Saving %s" % (filename,))
	fig.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')

	return
	# savefig takes a long time, so do it in a subprocess
	try:
		pid = os.fork()
	except OSError:
		pid = -1
	if pid == 0:
		try:
			fig.savefig(filename)
		except KeyboardInterrupt:
			pass
		sys.exit(0)
	elif pid == -1:
		print("Saving %s in current process" % (filename,))
		fig.savefig(filename)
	else:
		print("Saving %s in child %d" % (filename, pid))


def generate_plots(timings, X_axis, Y_axis, legend_keys, legend_format, line_format, plot_keys, plot_format):
	plot_values = defaultdict(lambda: defaultdict(list))

	line_formats = defaultdict(dict)
	legend_names = {}
	plot_names = {}

	for r in timings:
		legend_key = tuple(r[o] for o in legend_keys)
		if legend_key not in legend_names:
			legend_names[legend_key] = legend_format(r)

		plot_key = tuple(r[o] for o in plot_keys)
		if plot_key not in plot_names:
			plot_names[plot_key] = plot_format(r)

		if legend_key not in line_formats[plot_key]:
			line_formats[plot_key][legend_key] = line_format(r)

		plot_values[plot_key][legend_key].append((X_axis[1](r), Y_axis[1](r)))

	keys = sorted(plot_values.keys())

	for i, plot_key in enumerate(keys):
		plot_name = plot_names[plot_key]

		fig = plt.figure()
		ax = fig.add_subplot(1, 1, 1, title=plot_name)
		xs = []
		ys = []
		for legend_key, values in plot_values[plot_key].items():
			x = list(map(lambda v: v[0], values))
			y = list(map(lambda v: v[1], values))
			xs += x
			ys += y

			same_x = defaultdict(list)
			for xv, yv in values:
				same_x[xv].append(yv)

			name = legend_names[legend_key]
			color, marker, face_color = line_formats[plot_key][legend_key]
			ax.plot(x, y, marker, color=color, label=name, mfc=face_color)

			for xv, yvs in same_x.items():
				if len(yvs) == 1:
					continue

				avg = sum(yvs) / len(yvs)

				ax.errorbar([xv], [avg], yerr=[[avg - min(yvs)], [max(yvs) - avg]], fmt='none', capsize=10, barsabove=True, color=color)

		ax.set_xlabel(X_axis[0])
		ax.set_ylabel(Y_axis[0])

		ax.semilogx(basex=2)
		ax.xaxis.set_major_formatter(LogFormatterSciNotation(2))
		# ax.set_xticks(xs)

		lgd = ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
		ax.grid()

		fig.tight_layout()

		savefig(fig, lgd, '%s/%s.pdf' % (OUTPUT_DIR, plot_name))

		plt.close(fig)


Y_axis = ('duration (s)', lambda r: r['duration'])
X_axis = ('block_size (bytes)', lambda r: r['block_size'])
legend_keys = ['old_streams', 'compression', 'readahead']


def legend_format(t):
	return ''.join([
			'old' if t['old_streams'] else 'new',
			' compression' if t['compression'] else '',
			' readahead' if t['readahead'] else ''])


def line_format(t):
	marker = 'x' if t['old_streams'] else 'o'
	rc = t['readahead'] * 2 + t['compression']
	colors = CB_color_cycle

	return (colors[rc], marker + '--', None if t['old_streams'] else 'none')


plot_keys = ['test', 'item_type', 'parameter']


def plot_format(t):
	test_names = ['write_single', 'write_single_chunked', 'read_single', 'read_back_single', 'merge', 'merge_single_file', 'distribute', 'binary_search']
	test_name = test_names[t['test']]

	item_names = ['int', 'std::string', 'keyed_struct']
	item_name = item_names[t['item_type']]

	name = '%s: %s' % (test_name, item_name)
	if t['parameter'] != 0:
		name += ', k = %s' % t['parameter']

	return name


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('file')
	args = parser.parse_args()

	try:
		os.unlink('allplots.pdf')
	except FileNotFoundError:
		pass

	shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
	os.mkdir(OUTPUT_DIR)

	timings = []
	with open(args.file, 'r') as f:
		timings = [json.loads(l) for l in f]

	generate_plots(timings, X_axis, Y_axis, legend_keys, legend_format, line_format, plot_keys, plot_format)

	plot_files = sorted(Path(OUTPUT_DIR).iterdir(), key=lambda p: p.lstat().st_ctime)

	print('Creating allplots.pdf')
	check_call(['pdfunite'] + plot_files + ['allplots.pdf'])
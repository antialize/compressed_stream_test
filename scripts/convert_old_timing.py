from peewee import SqliteDatabase, Model, IntegerField, BooleanField, DoubleField
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('database')
parser.add_argument('output')
args = parser.parse_args()

db = SqliteDatabase(args.database)

keys = ['old_streams', 'block_size', 'file_size', 'compression', 'readahead', 'item_type', 'test', 'parameter', 'duration', 'timestamp']
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


# XXX: Due to a bug in the old speedtest script
# the values in old_streams column is reversed.
# This wrapper class fixes it.
class TimingWrapper:
	def __init__(self, timing):
		self.timing = timing

	def __getattr__(self, key):
		if key == 'old_streams':
			return not getattr(self.timing, 'old_streams')
		else:
			return getattr(self.timing, key)



db.connect()
with open(args.output, 'w') as f:
	for t in map(TimingWrapper, Timing.select()):
		d = {k: getattr(t, k) for k in keys}
		json.dump(d, f)
		f.write('\n')

db.close()

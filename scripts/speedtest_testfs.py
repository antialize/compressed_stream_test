items = 1
tests = 1

DEBUG = False
SHOULD_KILLCACHE = True
SHOULD_FORMAT = True
SHOULD_VALIDATE = False
action_args = range(3) if SHOULD_VALIDATE else range(2)

MB = 2**20
min_bs = MB // 16
max_bs = 4 * MB

# In megabytes
min_fs = 1024
max_fs = 128 * 1024

blocksizes = list(exprange(min_bs, max_bs))
filesizes = list(exprange(min_fs, max_fs))

compression_args = [False]
readahead_args = [False]
item_args = range(items)
test_args = range(tests)

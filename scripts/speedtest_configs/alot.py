DEBUG = False
SHOULD_KILLCACHE = True
SHOULD_FORMAT = True
SHOULD_VALIDATE = False
action_args = range(3) if SHOULD_VALIDATE else range(2)

MB = 2**20
min_bs = MB // 16
max_bs = 4 * MB

# In megabytes
min_fs = max_fs = 2**16

blocksizes = list(exprange(min_bs, max_bs))
filesizes = list(exprange(min_fs, max_fs))

compression_args = bins
readahead_args = bins
item_args = list(range(items))
test_args = list(range(tests))

DEBUG = False
SHOULD_KILLCACHE = True
SHOULD_FORMAT = True
SHOULD_VALIDATE = False
action_args = range(3) if SHOULD_VALIDATE else range(2)

MB = 2**20
min_bs = max_bs = 2 * MB

# In megabytes
min_fs = max_fs = 1024

blocksizes = list(exprange(min_bs, max_bs))
filesizes = list(exprange(min_fs, max_fs))

compression_args = [False]
readahead_args = [False]
item_args = [0]
test_args = [0]
merge_params = [2]
#job_args = [1]

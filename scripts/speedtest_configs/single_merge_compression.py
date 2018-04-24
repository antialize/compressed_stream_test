DEBUG = False
SHOULD_KILLCACHE = True
SHOULD_FORMAT = True
SHOULD_VALIDATE = False
action_args = range(3) if SHOULD_VALIDATE else range(2)

TEST_RUNS = 1000

MB = 2**20
min_bs = max_bs = 2 * MB

# In megabytes
min_fs = max_fs = 2**16

blocksizes = list(exprange(min_bs, max_bs))
filesizes = list(exprange(min_fs, max_fs))

compression_args = [1]
readahead_args = bits
item_args = [2]
test_args = [4]
merge_params = [64]
job_args = [1]

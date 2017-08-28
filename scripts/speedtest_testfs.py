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

blocksizes = list(exprange(min_bs, max_bs))
filesizes = list(exprange(2 ** 10, 2 ** 17))
compression_args = [False]
readahead_args = [False]
item_args = range(items)
test_args = range(tests)

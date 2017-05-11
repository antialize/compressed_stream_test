# Rewrite of TPIE file stream API

Motivation
--

Current TPIE implementation of file streams has a number of problems:

- Compressed streams doesn't work in some cases (race condition)
- Different API for POD streams and serialized streams
- Different classes for reading/writing forward/backwards for serialized streams (4 different classes in total)
- No compression support for serialized streams
- No readahead


Features wanted
--

- Correctness
- Files and streams - allow multiple streams for one file, acts as different view into the same file
- Allow all combinations of compressed/uncompressed POD/serialized items with the same API
- Read ahead/back
- 1 branch read/write
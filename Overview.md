# Overview of implementation

File format
==

Every file has the following format:

```
[file header][user data][block 0][block 1]...[block n-1]
```

where `n` is the number of blocks stored in the file.

File header
--

The \ref file_header is a struct containing some information about the file most importantly the total number of blocks, whether the file is compressed and/or serialized and the size of the user data block.

User data
--

When creating a file the user can specify that it wants to reserve some space for some extra data not stored in the blocks. The maximum size of this user data cannot be changed once the file has been created, as that would require moving all the blocks.

The user can read and write to this area by using specialized functions (read_user_data and write_user_data).

Blocks
--

Every block has the following format:

```
[block header 1][data][block header 2]
```

The two headers are identical in contents and we need them both to support reading both forwards and backwards.

The header contains the following information:

- `logical_offset`: The offset of the first item in the block. The first block has logical offset 0, the next has logical offset equal to block 0's `logical_size`
- `physical_size`: The physical size of the block in the file including both headers. The size of the data is `physical_size - 2*sizeof(block_header)`
- `logical_size`: The number of logical items in the block. If the block contains 10 ints this would be 10.

There should never be a block with `logical_size` 0 in a file, as we would just remove it.

Opening a file
==

When a file is opened we read the file header and check that every field is as we expect and then we read in the last block of the file. A file must always have a pointer to its last block as we use it to decide when to create a new block, when writing past it, but also to determine the size of the file. It is also used to seek to the end of the file. It would be technically possible to not have this requirement, but it seems to be really hard to implement it and cover all edge-cases.

Block pool
==

Every file can have a number of streams associated with it, where each stream can point to different parts of the file and they are used to read and write to the file.

Normally you only need one stream per file, but we support multiple streams per file.

The main building block of the representation of a file in memory is its blocks. We have a global pool of blocks for all files that are not currently in use stored in `available_blocks`. These can be either blocks that has not been used yet or blocks that was once used, but is not needed right now. Whenever a block not currently in memory is actually needed it is first removed from `available_blocks` and then configured to represent this new block. Every block is reference counted and will be put back in `available_blocks` once its use reaches 0. When this happens we don't clear the data and metadata for the block as if we need the block again later and it has not been repurposed meanwhile, we don't need to read the block from the disk, but can just reuse it directly.

To support this every file has a map from block numbers to blocks, where every block in memory for this file is stored, even the once whose use count is 0.

For every IO worker thread, every file and every stream we allocate 1 block to the pool. The thread/file/stream doesn't own a particular block, but just allocates 1 to the global pool. Each worker thread uses 1 block when reading/writing a block, a file always uses its last block and every stream uses the block for the current position of the stream. If readahead is enabled every stream also has another block used as the readahead block. When the thread/file/stream are destroyed they will then deallocate the same number of blocks they allocated to the pool.

Reading a file
==

...

IO worker threads
==

The IO worker threads are there to do IO, compression and serialization.

A worker can receive 4 kinds of jobs: read, write, truncate and terminate.

The terminate job just makes the thread finish exectuting and is only used when `file_stream_term` is called.


Readahead/back
==

To support readahead/back every stream can have a pointer to an extra block, which will be preloaded before the stream reaches that block.

...



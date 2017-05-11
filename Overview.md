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

- `logical_offset`: The offset of the block. The first block is at offset 0, the next is at offset 1 etc.
- `physical_size`: The physical size of the block in the file including both headers. The size of the data is `physical_size - 2*sizeof(block_header)`
- `logical_size`: The number of logical items in the block. If the block contains 10 ints this would be 10.

There should never be a block with `logical_size` 0 in a file, as we would just remove it.

What happens when a file is opened 
==

When a file is opened we read the file header and check that every field is as we expect and then we read in the last block of the file. A file must always have a pointer to its last block as we use it to decide when to create a new block, when writing past it, but also to determine the size of the file. It is also used to seek to the end of the file. It would be technically possible to not have this requirement, but it seems to be really hard to implement it and cover all edge-cases.

What happens when we read from a file
==
...

What happens when we write to a file
==
...
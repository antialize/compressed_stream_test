
Features wanted
---------------

- Correctness
- Files and streams
- Serialization support
- Read ahead/back
- 1 branch read/write


Simplified list of public classes
---

```c++
class block_base {...};

class file_impl; /* opaque */
class stream_impl; /* opaque */

class file_base {
  friend class file_impl;
  file_impl * m_impl;
  ...
};
class stream_base {
  friend class stream_impl;
  stream_impl * m_impl;
  ...
};

class file<T> : file_base {...};
class stream<T> : stream_base {...};
```

# Range Query Driven Compaction for RocksDB

We have implemented a range query driven compaction for RocksDB. It helps in compacting the files that overlap within a specified key range, resulting in improved query performance. The range query driven compaction is designed to be called when a range query comes in, and it is up to the developer to call this function from the API.

## Usage

To use the range query driven compaction, call the `RangeQueryDrivenCompaction` function with the desired start and end keys:

```cpp
DB* db;
Slice _start_key("key1");
Slice _end_key("key100");
db->RangeQueryDrivenCompaction(_start_key, _end_key);
```

<br>
<br>

## Our Contribution

1. [Examples README.md](./examples/README.md)
2. [SCRIPTS README.md](./scripts/README.md)

## Report

- [Final Report](./FINAL_REPORT.pdf)

<br>
<br>

---

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/main/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Questions and discussions are welcome on the [RocksDB Developers Public](https://www.facebook.com/groups/rocksdb.dev/) Facebook group and [email list](https://groups.google.com/g/rocksdb) on Google Groups.

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.

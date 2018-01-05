CORES
=====
Introduction
-----

    This project, CORES (Column-Oriented Regeneration Embedding Scheme), aims at pushing high-selective filters down into the column-based storage, where each filter consists of several filtering conditions on a field. By applying the filtering conditions to the column scan in storage, it tends to reduce both the I/O and the deserialization cost by introducing a fine-gained composition based on bitset. It also generalizes this technique by two pair-wise operations rollup and drilldown, such that a series of conjunctive filters can effectively deliver their payloads in nested schema. It can be applied to the nested relational model especially when hierarchical entities are frequently required by adhoc queries.
    This code is released under the Apache License, See LICENSE.txt and NOTICE.txt for more info.

Important Implementations
-----

1.`cores.avro.FilterBatchColumnReader`, the class that reads the cores files with filters. It conducts the columns about filters, initializes a bitset, delivers it through the scheme path, and then reads the fetching columns according the bitset.

2.`cores.avro.FilterOperator`,the interface that defines two functions of filters, getName() and isMatch(T t). getName() returns the name of the filter column, isMatch(T t) returns whether t is hitted by the filter.

3.`cores.avro.mapreduce.NeciFilterRecordReader`, the class that extends org.apache.hadoop.mapreduce.RecordReader, uses cores.avro.FilterBatchColumnReader to read the cores files in HDFS.

4.`cores.core.UnionOutputBuffer/UnionInputBuffer`, the class that writes/reads the columns with UNION type.

test

Contains the test framework. Several tests are making sure the examples runs. The test framework uses TestNG.

State-of-the-art Comparison
-----

[project](https://github.com/liyang0920/cores "https://github.com/liyang0920/cores")

[cores test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/cores/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/cores/query")

[avro test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/avro/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/avro/query")

[trevni test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/trevni/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/trevni/query")

[parquet test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/parquet/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/parquet/query")

[Spark with hdfs/parquet/json/avro](https://github.com/lwhay/spark-tpch/tree/parquet/src/main/scala/cores "https://github.com/lwhay/spark-tpch/tree/parquet/src/main/scala/cores")

[Hive with MapReudce and Tez]


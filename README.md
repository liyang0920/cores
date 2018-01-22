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

[Hive with MapReudce and Tez](https://github.com/whulyx/hive-experiment "https://github.com/whulyx/hive-experiment")



Appendex: Workloads and their parameters
-----
### A. DETAILS OF MODIFIED TPCH QUERIES.
#### A.1 MQ6 and its variant parameters.
SELECT SUM(l.l_extendedprice * l.l_discount) as revenue

FROM LineItem AS l

WHERE l.l_shipdate BETWEEN @date1 AND @date2

AND l.l_discount BETWEEN @discount1 AND @discount2

AND l.l_quantity <= @quantity;

Table 1: Different selectivity by varying selection conditions in query MQ6.

Variables| @date1| @date2| @discount1| @discount2 |@quantity
--- | --- | --- | --- |--- |---
0.1 |"1992-01-01"| "1994-01-01"| 0 |0.04 |40
0.01 |"1993-01-01"| "1994-01-01"| 0.02| 0.04| 13
0.001| "1993-10-10"| "1994-01-01"| 0.03 |0.04 |9
0.0001| "1993-11-01" |"1994-01-01"| 0.04| 0.04| 3
0.00001| "1992-12-19"| "1994-01-01"| 0.04| 0.04| 2
#### A.2 MQ14 and its variant parameters.
SELECT p.p_type, l.l_extendedprice, l.l_discount

FROM Part AS p, LineItem AS l

WHERE p.p_partkey = l.l_partkey

AND l.l_shipdate >= @date1

AND l.l_shipdate <= @date2;

Table 2: Different selectivity by varying selection conditions in query MQ14.

Variables| @date1| @date2
--- | --- | --- 
0.1 |"1993-05-01"| "1994-01-01"
0.01 |"1993-11-01"| "1994-01-01"
0.001| "1991-11-25"| "1992-01-26"
0.0001| "1991-11-21" |"1992-01-09"
0.00001| "1992-11-25"| "1992-01-04"
#### A.3 MQ19 and its variant parameters.
SELECT l.l_extendedprice * l.l_discount

FROM Part AS p, LineItem AS l

WHERE p.p_partkey = l.l_partkey

AND p.p_brand <= @brand

AND p.p_container IN @cntset

AND p.p_size <= @size

AND l.l_shipinstruct = "DELIVER IN PERSON"

AND l.l_quantity BETWEEB @qty1 AND @qty2

AND l.l_shipmode IN @modeset;

Table 3: Different selectivity by varying selection conditions in query MQ19.

Variables| @brand| @cntset| @size| @qty1 |@qty2|@modeset
--- | --- | --- | --- |--- |--- |---
0.1 |"Brand#50"| {"SM","MED","LG","WRAP"}| 50 |0 |50|{"TRUCK","AIR","SHIP","FOB"}
0.01 |"Brand#40"| {"SM","MED","LG"}| 30 |10 |30|{"TRUCK","AIR","SHIP"}
0.001|"Brand#20"| {"SM","MED"}| 20 |10 |30|{"TRUCK","AIR"}
0.0001|"Brand#20"| {"SM"}| 20 |10 |20|{"TRUCK"}
0.00001|"Brand#14"| {"SM"}| 5 |10 |15|{"TRUCK"}
#### A.4 MQ20 and its variant parameters.
SELECT ps.ps_suppkey, ps.ps_availqty

FROM Part AS p, PartSupp AS ps, LineItem AS l

WHERE p.p_partkey = ps.ps_partkey

AND ps.ps_partkey = l.l_partkey

AND ps.ps_suppkey = l.l_suppkey

AND p.p_name IN @nameset

AND l.l_shipdate >= @shipdate1

AND l.l_shipdate <= @shipdate2;

Table 4: Different selectivity by varying selection conditions in query MQ20.

Variables| @nameset| @shipdate1| @shipdate2
--- | --- | --- | --- 
0.1 | {"green","lemon","red"}| "1992-01-01" |"1998-01-01"
0.01 | {"green"}| "1992-09-10" |"1993-01-01"
0.001|  {"sandy"}| "1992-10-20" |"1992-12-10"
0.0001 | {"sandy"}| "1992-12-05" |"1992-12-10"
0.00001 | {"sandy"}| "1992-10-24" |"1998-12-10"
### B. Modified PTCH QUERIES ON NESTED SCHEMAS.
#### B.1 P-PS-L: nested schema for Part, Partsupp, Lineitem with seudo mapping scheme.
{"type": "record", "name": "PPSL",

 "fields": [

     {"name": "p_partkey", "type": "int", "order": "ignore"},

     {"name": "p_name", "type": "string"},

     {"name": "p_mfgr", "type": "string"},

     {"name": "p_brand", "type": "string"},

     {"name": "p_type", "type": "string"},

     {"name": "p_size", "type": "int"},

     {"name": "p_container", "type": "string"},

     {"name": "p_retailprice", "type": "float"},

     {"name": "p_comment", "type": "string"},

	    {"name": "pslst", "type":{"type": "array",

	     "items":{"type": "record", "name": "ps",

	     "fields": [

	         {"name": "ps_partkey", "type": ["int", "null"], "order": "ignore"},

		        {"name": "ps_suppkey", "type": ["int", "null"]},

	       	 {"name": "ps_availqty", "type": ["int", "null"]},

	       	 {"name": "ps_supplycost", "type": ["float", "null"]},

	       	 {"name": "ps_comment", "type": ["string", "null"]},

		        {"name": "llst", "type":{"type": "array",

		        "items": {"type": "record", "name": "l",

	        "fields": [

		            {"name": "l_orderkey", "type": "int", "order": "ignore"},

			           {"name": "l_partkey", "type": "int"},

		          	 {"name": "l_suppkey", "type": "int"},

		          	 {"name": "l_linenumber", "type": "int"},

          			 {"name": "l_quantity", "type": "float"},

	          		 {"name": "l_extendedprice", "type": "float"},

		          	 {"name": "l_discount", "type": "float"},

		          	 {"name": "l_tax", "type": "float"},

			           {"name": "l_returnflag", "type": "bytes"},

		          	 {"name": "l_linestatus", "type": "bytes"},

			           {"name": "l_shipdate", "type": "string"},

		          	 {"name": "l_commitdate", "type": "string"},

			           {"name": "l_receiptdate", "type": "string"},

		          	 {"name": "l_shipinstruct", "type": "string"},

		          	 {"name": "l_shipmode", "type": "string"},

		          	 {"name": "l_comment", "type": "string"}

          ]}}}

     ]}}}

]}

#### B.2 MQ06 on PPSL schema.
SELECT SUM(d.pslst.ps.llst.l.l_extendedprice * d.pslst.ps.llst.l.l_discount)

FROM PPSL AS d

WHERE d.pslst.any:ps.llst.any:l.l_shipdate BETWEEN @date1 AND @date2

AND d.pslst.any:ps.llst.any:l.l_discount BETWEEN @dc1 AND @dc2

AND d.pslst.any:ps.llst.any:l.l_quantity <= @quantity;
#### B.3 MQ14 on PPSL schema.
SELECT d.p_type, d.pslst.ps.llst.l.l_extendedprice, d.pslst.ps.llst.l.l_discount

FROM PPSL AS d

WHERE d.pslst.any:ps.llst.any:l.l_shipdate BETWEEN @date1 AND @date2;
#### B.4 MQ19 on PPSL schema.
SELECT d.pslst.ps.llst.l.l_extendedprice * d.pslst.ps.llst.l.l_discount

FROM PPSL AS d

WHERE d.p_brand <= @brand AND d.p_container IN @cntset

AND d.p_size <= @size

AND d.pslst.any:ps.llst.any:l.l_shipinstruct = "DELIVER IN PERSON"

AND d.pslst.any:ps.llst.any:l.l_quantity BWTWEEN @qty1 AND @qty2

AND d.pslst.any:ps.llst.any:l.l_shipmode IN @modeset;
#### B.5 MQ20 on PPSL schema.
SELECT d.pslst.ps.ps_suppkey, d.pslst.ps.llst.l.ps_availqty

FROM PPSL AS d

WHERE d.p_name IN @nameset

AND d.pslst.any:ps.llst.any:l.l_shipdate BETWEEM @date1 AND @date2;

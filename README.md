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
### Codebase of the comparisons.
We choose two workloads for evaluating. In the first one, three tables of TPCH, i.e., Lineitem, Partsupp and Part, are used
to build a three-level nested schema as given in Figure 9. We modify four queries Q-6, Q-14, Q-19 and Q-20 to evaluate their filtering and fetching components with different selectivities. These modified queries are denoted by (TPCH-)MQ6, MQ14, MQ19
and MQ20. The second one includes 12 million Pubmed records, which can be obtained from Medline Website. As shown in Table 1, it has a complex schema with more than one hundred nodes.

All our experiments are evaluated on a single-node server and a cluster. The single-node server, namely, Dell PowerEdge R730, has two Intel Xeon ES-2640 CPUs with 8 cores each, and 320GB SCSI disk with about 180MB/s I/O throughput. We investigate the I/O behaviors of CORES, and compare it with three competitors, including AsterixDB-0.9.2, Avro/Trevni-1.9 and Parquet-1.6. Our cluster is connected by a 40Gb/s InfiniBand network, each node consists of two AMD Operon(tm) 6174 CPUs with 12 cores each, run at 800MHz and 128GB memory. We introduce Hadoop-2.7 as our running environment to compare CORES to Hive-3.0.0 (run on both MapReduce and Tez-0.9), Spark-2.0 and AsterixDB-0.9.2 in the MapReduce environment. We use 32KB block size in CORES-M(C) and conduct the default settings for their competitors, where Avro/Trevni are evaluated with 64KB blocks and the size of row group in Parquet is 128MB.

To measure the efficiency of CORES, three data formats, i.e., Avro, Trevni and Parquet, are conducted in our experiments. In addition, we also measure CORES compared to several big-data platform (Spark, Hive/Tez) in the MapReduce environments. The experimental codes are as follows.

[project](https://github.com/liyang0920/cores "https://github.com/liyang0920/cores")

[cores test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/cores/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/cores/query")

[avro test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/avro/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/avro/query")

[trevni test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/trevni/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/trevni/query")

[parquet test](https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/parquet/query "https://github.com/liyang0920/cores/tree/master/avro/src/test/java/local/parquet/query")

[Spark with hdfs/parquet/json/avro](https://github.com/lwhay/spark-tpch/tree/parquet/src/main/scala/cores "https://github.com/lwhay/spark-tpch/tree/parquet/src/main/scala/cores")

[Hive with MapReudce and Tez](https://github.com/whulyx/hive-experiment "https://github.com/whulyx/hive-experiment")

### Staged costs of CORES with cascading/merging schemes.
To detail the staged costs of CORES, we execute the queries with cold cache on the nested dataset of TPCH-S and Pubmed. In all the figures in this section, the runtime of CORES is divided into three parts from bottom up, respectively denoting the I/O, deserialization, and regeneration costs. In the following tables, the costs of each query are divided into three parts, i.e., IO, filtering, generation. Notice that the IOs are generally executed in parallel with filtering in modern operating systems.

Table 1: Staged costs of CORES-C/M with variant selecitivies on TPCH-MQ06 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time |14.96 | 35.03| 37.39 |40.88 |41.55     |8.23 | 11.89 | 11.51 |12.28 |13.01 
filtering |35.26 | 16.56| 14.06| 10.81| 10.09   |56.05 | 55.90| 55.60 |53.99 |53.94 
generation| 5.88 |1.89| 1.48 |0.81 |0.23        |5.55 | 1.93  | 1.52 |0.78 |	0.26
total|56.11  | 53.48 | 52.93  |52.50 |51.87     |69.83 |	69.73| 68.62  |67.06  |	67.20 


Table 2: Staged costs of CORES-C/M with variant selecitivies on TPCH-MQ14 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time |19.59 |18.13 |20.18 |20.38 |22.26 |19.59 |18.13 |20.18 |20.38 |22.26 
filtering |37.44 |38.12 |36.49 |36.31 |36.54 |37.44 |38.12 |36.49 |36.31 |36.54 
generation| 5.88 |1.89| 1.48 |0.81 |0.23        |5.55 | 1.93  | 1.52 |0.78 |	0.26 
total | 72.65 |60.89 |59.37 |58.57 |59.89        |72.65 |60.89 |59.37 |58.57 |59.89 


Table 3: Staged costs of CORES-C/M with variant selecitivies on TPCH-MQ19 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time |29.36 |49.45 | 56.68 | 59.46 | 63.43 |19.24 |24.33 | 22.56 | 24.50 |22.44 
filtering |51.49 |15.04 |7.34 |5.44 |3.44 |88.80 |85.67 |84.62 |82.28 |81.93 
generation|5.66 |2.08 | 1.50 |0.90 |0.28 |5.76 |1.94 | 1.54 |0.90 |0.28 
total | 86.52 |66.57 |65.52 |65.80 |67.14 |113.79 |111.94 |108.72 |107.68 |104.65 


Table 4: Staged costs of CORES-C/M with variant selecitivies on TPCH-MQ20 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time |22.74 |26.62 |29.08 |29.85 |10.51 |5.53 |9.15 |8.14 |8.14 |9.36 
filtering |11.26 |6.70 |4.13 |4.20 |2.17 |41.05 |38.57 |39.25 |39.28 |38.47 
generation|2.52 |1.41 |1.30 |0.95 |0.78 |2.61 |1.32 |1.17 |0.88 |0.72 
total | 36.52 |34.73 |34.51 |35.00 |13.46 |49.20 |49.04 |48.56 |48.30 |48.54 


Table 5: Staged costs of CORES-C/M with variant selecitivies on Pubmed-Q1 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time | 3.92 |3.10 |1.42 |0.74 |0.49 |3.17 |2.59 |0.68 |0.11 |0.12 
filtering |1.97 |1.86 |1.69 |1.63 |1.65 |3.06 |2.97 |2.99 |2.96 |2.80 
generation|1.16 |0.36 |0.20 |0.12 |0.12 |1.03 |0.36 |0.17 |0.12 |0.14 
total | 7.05 |5.33 |3.31 |2.49 |2.26 |7.26 |5.93 |3.85 |3.19 |3.05 


Table 6: Staged costs of CORES-C/M with variant selecitivies on Pubmed-Q2 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time | 1.05 |1.01 |1.06 |0.54 |0.67 |0.50 |0.97 |0.68 |0.26 |0.34 
filtering | 1.70 |1.77 |1.62 |1.58 |1.59 |2.03 |1.99 |1.87 |1.92 |1.98 
generation|0.38 |0.17 |0.14 |0.13 |0.11 |0.40 |0.20 |0.13 |0.11 |0.10 
total | 3.13 |2.95 |2.82 |2.25 |2.37 |2.93 |3.16 |2.69 |2.29 |2.41 


Table 7: Staged costs of CORES-C/M with variant selecitivies on Pubmed-Q3 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time | 4.26 |2.76 |2.21 |2.23 |1.73 |3.33 |1.99 |1.72 |1.81 |1.11 
filtering | 3.89 |2.49 |2.41 |2.35 |2.08 |7.55 |7.32 |7.01 |8.40 |8.49 
generation|0.99 |0.31 |0.22 |0.33 |0.11 |1.06 |0.32 |0.17 |0.04 |0.11 
total | 9.14 |5.56 |4.83 |4.91 |3.93 |11.94 |9.63 |8.90 |10.26 |9.71 


Table 8: Staged costs of CORES-C/M with variant selecitivies on Pubmed-Q4 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time | 15.12 |3.19 |2.05 |1.37 |0.91 |13.67 |2.66 |0.94 |0.74 |0.53 
filtering | 1.85 |2.21 |1.84 |2.02 |1.95 |2.79 |4.11 |4.14 |4.34 |4.14 
generation|3.71 |0.03 |0.16 |0.12 |0.12 |3.60 |0.21 |0.13 |0.12 |0.10 
total | 20.69 |5.42 |4.05 |3.51 |2.97 |20.06 |6.98 |5.21 |5.20 |4.78 

### Ingestion costs of CORES compared to Trevni and Avro.
In this experiment, using a total selectivity of 100% in all queries, we measure the overhead of CORES with/without filtering. In the following table, we list the runtime of CORES (with/without) filtering, and compare them with Avro/Trevni with the same payloads of the modified TPCH queries. In this table, the columns associated with -Cold and -Hot are measured  on disk and in memory, respectively.

Table 9: Runtime of CORES with/without filtering compared to Avro/Trevni (in seconds).

Threshold| MQ06| MQ14| MQ19| MQ20 | MQ06| MQ14| MQ19| MQ20 
--- | --- | --- | --- |--- |---| --- | --- | --- 
Storage| -Cold | -Cold | -Cold|-Cold  | -Hot |-Hot   |-Hot  |-Hot   
CORES-Filter | 94.81 |94.53 |129.13 |51.49 |88.33 |85.59 |120.10 |46.44 
CORES-No-Filter | 119.01 |117.28 |270.37 |74.74 |91.86 |84.51 |255.86 |64.33
Trevni|169.70 |86.03 |243.53 |59.88 |170.39 |72.84 |229.84 |58.61 
Avro | 282.01 |257.60 |306.90 |257.08 |262.04 |153.01 |285.99 |174.62 

### Total costs of CORES compared to Parquet Trevni and Avro.
This experiments give the comparison results based on Pubmed regarding all costs of the queries with variant selectivities. It should be noticed that the superiority of CORES on TPCH is similar with what we obtained from Pubmed. We omit the results and their MapReduce-based results. People who desire more details can contact the authors.

Table 10: Total costs of CORES compared to Parquet, Trevni and Avro (in seconds) based on Pubmed.

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
Queries| Q1 | Q1 | Q1 | Q1  |Q1| Q2  |Q2   |Q2  |Q2 |Q2
Trevni | 12.39 |11.78 |11.48 |11.66 |11.36 |7.37 |7.55 |7.04 |7.11 |7.17  
Avro | 124.06 |125.15 |124.94 |124.78 |124.47 |123.88 |124.08 |124.04 |124.42 |124.50 
Parquet|25.18 |27.46 |24.96 |25.22 |25.38 |11.46 |13.27 |11.53 |11.36 |11.44 
CORES-C | 7.05 |5.33 |3.31 |2.49 |2.26 |3.13 |2.95 |2.82 |2.25 |2.37 
CORES-M | 7.26 |5.93 |3.85 |3.19 |3.05 |2.93 |3.16 |2.69 |2.29 |2.41 
Queries| Q3 | Q3 | Q3 | Q3  |Q3| Q4  |Q4   |Q4  |Q4 |Q4
Trevni | 19.96 |20.21 |19.48 |20.32 |20.54 |59.83 |59.87 |61.25 |59.51 |59.48 
Avro | 126.68 |125.91 |125.25 |126.05 |125.40 |124.22 |124.05 |124.07 |124.56 |124.27 
Parquet|41.94 |41.01 |41.19 |41.17 |40.71 |120.81 |118.44 |118.30 |119.10 |120.52 
CORES-C |9.14 |5.56 |4.40 |4.91 |3.93 |20.69 |5.42 |4.05 |3.51 |2.97 
CORES-M | 11.94 |9.63 |8.90 |10.26 |9.71 |20.06 |6.98 |5.21 |5.20 |4.78 

Appendex: Workloads and their parameters
-----
### A. DETAILS OF MODIFIED TPCH QUERIES.
#### A.1 MQ6 and its variant parameters.
SELECT SUM(l.l_extendedprice * l.l_discount) as revenue

FROM LineItem AS l

WHERE l.l_shipdate BETWEEN @date1 AND @date2

AND l.l_discount BETWEEN @discount1 AND @discount2

AND l.l_quantity <= @quantity;

Table 11: Different selectivity by varying selection conditions in query MQ6.

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

Table 12: Different selectivity by varying selection conditions in query MQ14.

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

Table 13: Different selectivity by varying selection conditions in query MQ19.

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

Table 14: Different selectivity by varying selection conditions in query MQ20.

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

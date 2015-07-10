# Succinct Performance Benchmark Tools

This is a performance testing framework for [Succinct](https://github.com/amplab/succinct).

## Constructing the dataset

Before running any benchmark, the dataset to be benchmarked against needs to be 
prepared. To do this, simply construct the corresponding Succinct 
data-structures and persist them to disk as follows:

```java
// Example for constructing SuccinctBuffer and persisting it to disk

// Read all of the file contents into a byte array
Path path = Paths.get("path/to/file");
byte[] data = Files.readAllBytes(path);

// Construct the Succinct data-structures
SuccinctBuffer buffer = new SuccinctBuffer(data);

// Persist the data structures to disk
buffer.writeToFile("path/to/output");
```

**Note that the construction is an expensive operation, and may take several
minutes to hours if the dataset is several gigabytes in size.**

## Running the benchmark

Before running the benchmark, you need to assemble the tool as follows:

```
mvn clean package
```

To run the benchmark, use the `succinct-perf` script provided in the `bin/` directory.

It's usage is as follows:

```
usage: succinct-perf
 -b <arg>   The benchmark to run; the specification format is
            <class-name>.<method-name>. If only class name is specified,
            then all benchmarks for that class name will be run. To run
            all benchmarks for all classes, specify "all"
 -d <arg>   Path to serialized Succinct data. (REQUIRED)
 -q <arg>   Path to query file that contains query strings (Required for
            search/count benchmarks).
 -r <arg>   Path where the results will be stored.
 -s <arg>   Storage mode for SuccinctBuffer benchmarks. Can be MEMORY_ONLY
            or MEMORY_MAPPED.
```

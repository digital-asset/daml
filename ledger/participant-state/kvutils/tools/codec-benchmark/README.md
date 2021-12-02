### Value and transaction coding benchmarks

#### Running

To run the benchmarks with the default input data, simply run:

    bazel run //ledger/participant-state/kvutils/tools:benchmark-codec
    
The default input data is the reference ledger export. It cannot be used as a
reliable source to compare performances between commits, as the content of the
export may vary over time. This should, however, provide a good dataset to
have a tight feedback loop on improvements to the translation between Speedy,
Daml-LF, and Protobuf objects and messages.

To run the benchmarks with a custom ledger export, you can run the following:

    bazel run //ledger/participant-state/kvutils/tools:benchmark-codec -- \
      -p ledgerExport=/absolute/path/to/a/ledger/export.bin
       
Please ensure to use an absolute path, as this will be run in a Bazel sandbox
whose working directory is unlikely to be yours.

There are two main use cases to pass a specific ledger export:

1. evaluate performance improvements over specific datasets, to address edge cases
2. compare the performance improvements of two commits using the same dataset so 
   that the outcomes are comparable

Note that each benchmark will consume the ledger export in its entirety, so it's
probably better to limit its size.

Note that `-p` is a [JMH](https://github.com/openjdk/jmh) option. You can see
all available options by running:

    bazel run //ledger/participant-state/kvutils/tools:benchmark-codec -- -h

JMH is mature and rich of features, ranging from setting up the run to gather
profiling information, persisting the results, tuning the JVM and much more.

You are **strongly** advised to have a look at the available options to make sure
you run the benchmarks that are relevant to you with the proper settings.
    
##### Examples

* Run only serialization benchmarks (see [Terminology](#terminology)) forking once,
  with default data, five warm-up iterations and ten measured iterations:

      bazel run //ledger/participant-state/kvutils/tools:benchmark-codec -- \
        -f 1 -wi 5 -i 10 ".*Deserialize.*"
 
* Run only transaction benchmarks forking once, with default data, five warm-up
  iterations and five measured iterations, forcing GC between iterations,
  reporting the average time:

      bazel run //ledger/participant-state/kvutils/tools:benchmark-codec -- \
        -f 1 -wi 5 -i 5 -gc true -bm avgt -tu ms ".*Transaction.*"
        
* List the benchmarks selected by the provided filter and exit:

      bazel run //ledger/participant-state/kvutils/tools:benchmark-codec -- -l ".*engine.*"

#### Terminology

* _serialize_: translate from a Protobuf object to its serialized form
* _deserialize_: translate a serialized Protobuf message into its object representation
* _encode_: translate a Protobuf object to a Daml-LF object
* _decode_: translate a Daml-LF object to a Protobuf object

#### Known issues

##### SLF4J warning

On the first warm-up iteration, a warning by SLF4J appears. This does not cause any
functional issue with the benchmarks. A first analysis seems to point at the need
to ensure the runtime dependencies (like the Logback Classic JAR) are correctly
forwarded to the JMH artifact ultimately produced by the Bazel `scala_benchmark_jmh`
rule.

##### Failure to run the benchmarks without an explicit export file on Windows

On Windows the build does not produce the reference ledger export. On Windows, you
must explicitly pass a ledger export to the benchmarks in order to run them.

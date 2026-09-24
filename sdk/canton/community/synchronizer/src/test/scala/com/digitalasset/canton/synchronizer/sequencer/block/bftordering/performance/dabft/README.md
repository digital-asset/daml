# DA BFT standalone benchmark tool

This directory contains a benchmark tool for a standalone DA BFT ordering network imported from the BFT evaluation
project and largely based on the [one for the Trantor prototype].

The [configuration file] is ready to execute against the [example standalone deployment].

Just deploy the example and run the `DaBftBenchmarlTool` class, for example from an IDE like IntelliJ IDEA.

It uses the regular Canton logging infrastructure, so logs are to be found in `logs/canton_test.log`.

[one for the Trantor prototype]: https://github.com/DACH-NY/bft-evaluation/tree/main/trantor/benchmark/loadgen-client
[configuration file]: ../../../../../../../../../../resources/bftbenchmark-dabft.conf
[example standalone deployment]: ../../../../../../../../../../../main/scala/com/digitalasset/canton/synchronizer/sequencer/block/bftordering/examples/deployment/standalone

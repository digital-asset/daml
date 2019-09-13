# quickstart-scala example

This example demonstrates how to:
- set up and configure Scala codegen (see `codegen` configuration in the `./daml.yaml`)
- instantiate a contract and send a corresponding create command to the ledger
- how to exercise a choice and send a corresponding exercise command  
- subscribe to receive ledger events and decode them into generated Scala ADTs

## Compile DAML project
The DAML code for the IOU example is located in the `./daml` folder. Run the following command to build it:
```
$ daml buld
```

## Start sandbox
This examples requires a running sandbox. To start it, run the following command:
```
$ daml sandbox ./.daml/dist/quickstart-0.0.1.dar
```
Where `./.daml/dist/quickstart-0.0.1.dar` is the DAR file created in the previous step.

## Compile and run Scala example
Run the following command from the `quickstart-scala` folder:
```
$ sbt "application/runMain com.digitalasset.quickstart.iou.IouMain localhost 6865"
```
If example completes successfully, the above process should terminate and the output should look like this:
```
<Sent and received messages>
...
11:54:03.865 [run-main-0] INFO - Shutting down...
...
[success] Total time: 7 s, completed Sep 12, 2019, 11:54:04 AM
```

To run the quickstart-scala as a standalone project (not part of the DAML project) or to override the default SDK version configured in the `./SDK_VERSION` file, you have to specify `da.sdk.version` JVM system properties:
```
$ sbt -Dda.sdk.version=<DA_SDK_VERSION> "application/runMain com.digitalasset.quickstart.iou.IouMain localhost 6865"
```

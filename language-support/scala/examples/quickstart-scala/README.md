# quickstart-scala example

This example demonstrates how to:
- set up and configure Scala codegen
- instantiate a contract and send a corresponding create command to the ledger
- how to exercise a choice and send a corresponding exercise command  
- subscribe to receive ledger events and decode them into generated Scala ADTs

This examples requires a running sandbox. To start a sandbox, run the following command from within a DAML Assistant project directory: 
```
$ daml start
```

To run the quickstart example:
```
$ sbt -Dda.sdk.version=<DA_SDK_VERSION> -Ddar.file=<DAR_FILE_PATH> "application/runMain com.digitalasset.quickstart.iou.IouMain localhost 6865"
```

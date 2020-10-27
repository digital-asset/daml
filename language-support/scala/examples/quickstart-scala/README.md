# quickstart-scala example

This example demonstrates how to:
- set up and configure Scala codegen (see `codegen` configuration in the `./daml.yaml`)
- instantiate a contract and send a corresponding create command to the ledger
- how to exercise a choice and send a corresponding exercise command
- subscribe to receive ledger events and decode them into generated Scala ADTs

All instructions below assume that you have the DAML Connect SDK installed. If you have not installed it yet, please follow these instructions: https://docs.daml.com/getting-started/installation.html

## Create a quickstart-scala project
```
daml new ./quickstart-scala --template quickstart-scala
```
This should output:
```
Created a new project in "./quickstart-scala" based on the template "quickstart-scala".
```
Where:
- `./quickstart-scala` is a project directory name
- `quickstart-scala` is a project template name, to see the entire list of available templates, run: `daml new --list`

## Compile the DAML project
The DAML code for the IOU example is located in the `./daml` folder. Run the following command to build it:
```
$ cd ./quickstart-scala
$ daml build
```
this should output:
```
Compiling quickstart to a DAR.
Created .daml/dist/quickstart-0.0.1.dar.
```

## Generate Scala classes representing DAML contract templates
```
$ daml codegen scala
```
This should generate scala classes in the `./scala-codegen/src/main/scala` directory. See `codegen` configuration in the `daml.yaml` file:
```
...
codegen:
  scala:
    package-prefix: com.daml.quickstart.iou.model
    output-directory: scala-codegen/src/main/scala
    verbosity: 2
```

## Start Sandbox
This examples requires a running sandbox. To start it, run the following command:
```
$ daml sandbox ./.daml/dist/quickstart-0.0.1.dar
```
where `./.daml/dist/quickstart-0.0.1.dar` is the DAR file created in the previous step.

## Compile and run Scala example
Run the following command from the `quickstart-scala` folder:
```
$ sbt "application/runMain com.daml.quickstart.iou.IouMain localhost 6865"
```
If example completes successfully, the above process should terminate and the output should look like this:
```
<Sent and received messages>
...
11:54:03.865 [run-main-0] INFO - Shutting down...
...
[success] Total time: 7 s, completed Sep 12, 2019, 11:54:04 AM
```

To run the quickstart-scala as a standalone project (not part of the DAML project), you can specify `da.sdk.version` JVM system properties:
```
$ sbt -Dda.sdk.version=<DA_SDK_VERSION> "application/runMain com.daml.quickstart.iou.IouMain localhost 6865"
```

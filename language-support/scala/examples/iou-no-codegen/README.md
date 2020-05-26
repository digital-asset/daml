# iou-no-codegen example

This example demonstrates how to:
- create a contract manually constructing a `Create` command and submitting it to the ledger
- subscribe to receive ledger events
- exercise a choice manually constructing an `Exercise` command and submitting to the ledger

This examples requires a running sandbox.

## To start a sandbox running IOU example
- create a new DAML Assistant `quickstart-scala` project
```
$ daml new quickstart-scala quickstart-scala
```
- change directory to this project
```
$ cd quickstart-scala
```
- compile DAR, start sandbox and navigator processes
```
$ daml start
```

## To run the iou-no-codegen example:
- Run sbt command from `iou-no-codegen` folder:
```
$ sbt "application/runMain com.daml.quickstart.iou.IouMain <sandbox-host-name> <sandbox-port-number> <iou-package-id>"
```

Default sandbox port is 6865.

IOU template package ID (`iou-package-id`) can be found in the Navigator. Templates screen displays all available contract templates, e.g:
```
Iou:Iou@fc3e49291d12ef5f46a3b51398558257a469884c46211942c1559cf0be46872c
Iou:IouTransfer@fc3e49291d12ef5f46a3b51398558257a469884c46211942c1559cf0be46872c
IouTrade:IouTrade@fc3e49291d12ef5f46a3b51398558257a469884c46211942c1559cf0be46872c
```
Package ID is the part of the template ID after `@` character. In the above example, all templates are from the package with package ID: `fc3e49291d12ef5f46a3b51398558257a469884c46211942c1559cf0be46872c`.

To connect to the sandbox running on localhost, listening to the default port and with the above package ID:
```
$ sbt "application/runMain com.daml.quickstart.iou.IouMain localhost 6865 fc3e49291d12ef5f46a3b51398558257a469884c46211942c1559cf0be46872c"
```

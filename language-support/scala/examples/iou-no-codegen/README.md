# iou-no-codegen example

This example demonstrates how to:
- create a contract manually constructing a `Create` command and submitting it to the ledger
- subscribe to receive ledger events
- exercise a choice manually constructing an `Exercise` command and submitting to the ledger

This examples requires a running sandbox. To start a sandbox, run the following command from within a DAML Assistant project directory: 
```
$ daml start
```

To run the iou-no-codegen example:
```
$ sbt "application/runMain com.digitalasset.quickstart.iou.IouMain localhost 6865 <iou-package-id>"
```

You can find `iou-package-id` in the Navigator's, Templates screen. It displays all available contract templates, e.g:
```
Iou:Iou@adb51bfb8212a831aaf6ddf75322254511c07d21c9339c1374a2e8236e769ed9
Iou:IouTransfer@adb51bfb8212a831aaf6ddf75322254511c07d21c9339c1374a2e8236e769ed9
IouTrade:IouTrade@adb51bfb8212a831aaf6ddf75322254511c07d21c9339c1374a2e8236e769ed9
```
Package ID is the last part of the Template ID, the one after `@` character. In the above example, `iou-package-id` is `adb51bfb8212a831aaf6ddf75322254511c07d21c9339c1374a2e8236e769ed9`.


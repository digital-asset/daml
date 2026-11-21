Updating canton-builtin-admin-workflow-party-replication-alpha.dar
==================================================================

The canton-builtin-admin-workflow-party-replication-alpha.dar implements a
propose-accept pattern to reach agreement among participants on how to perform
online party replication.

When you perform changes to the Daml model, you need to make sure that the changes
are backward compatible according to the smart contract upgrading (SCU) constraints,
e.g. by uploading the modified dar to a participant started with the prior version:

```
$ ./bin/canton -c examples/01-simple-topology/simple-topology.conf --bootstrap examples/01-simple-topology/simple-ping.canton
@ participant1.dars.upload("community/participant/target/scala-2.13/classes/canton-builtin-admin-workflow-party-replication-alpha.dar")
```

and ensuring that there is no error such as
`INVALID_ARGUMENT/NOT_VALID_UPGRADE_PACKAGE(8,..): Upgrade checks indicate that .. cannot be an upgrade of ...`.

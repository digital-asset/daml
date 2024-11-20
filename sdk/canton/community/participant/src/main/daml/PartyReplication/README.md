Party Replication Daml Admin Workflow
=====================================

The PartyReplication.daml admin workflow implements a propose-accept pattern
to reach agreement among participants on how to perform online party replication.

PartyReplication.dar is separate from the pinned AdminWorkflows.dar because party
replication is meant to be upgraded via the smart contract upgrade mechanism.

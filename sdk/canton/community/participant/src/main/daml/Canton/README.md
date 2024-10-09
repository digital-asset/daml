Updating the Daml Admin Workflows
=================================

When you perform changes to the admin workflows, such as the ping or DAR
distribution, you need to updated the pre-packaged DARs. The admin workflow DARs
are updated using `sbt damlUpdateFixedDars` and they have to be committed to the
repository.

We cannot change the admin workflows within the same major version as that would break compatibility.

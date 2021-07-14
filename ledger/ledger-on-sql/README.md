# Ledger on SQL

This is an implementation of a ledger using _kvutils_ on top of an SQL database. Currently, it can
be run on top of H2, PostgreSQL, or SQLite.

The code under _src/main_ implements the logic, using _kvutils_. This code is production-ready and
used by _Sandbox_.

The code under _src/app_ is a trivial application front-end to the ledger that spins it up, along
with a Ledger API Server. This is _not_ intended to be used in production, and is currently only
used in testing.

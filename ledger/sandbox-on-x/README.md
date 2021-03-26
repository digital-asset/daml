# Overview

Sandbox on X - Proof of Concept

This is a proof-of-concept of the sandbox-on-x architecture, which is a Daml ledger with persistency that uses the on-X architecture while being backed by an IndexDB only. This architecture is DB compatible with the sandbox-classic, while using the very same `participant.state.api.v1.ReadService` and `.WriteService` interfaces that all Daml ledger other than sandbox-classic use.

We pursue several goals with the implementation of the sandbox-on-x architecture:
1. Use it as a drop-in-replacement of the implementation of daml-on-sql and sandbox-classic; and thereby enable a massive code cleanup due to removing the direct DB-access code used in daml-on-sql and sandbox-classic.
2. Use it as the regression test for both functional and non-functional properties of the Ledger API server components built in the `daml` repo. We expect this implementation to satisfy the 1k CHESS+ trade/s throughput requirement.
3. Lower Spider's CI times by providing them with a high-throughput single-process Daml ledger.

The proof-of-concept should have the same throughput as any other Daml ledger, and can be used for testing the throughput of Ledger API server components. Its main limitation is that it does not do conflict-checking. We plan to resolve that in the future using a two-stage approach, which first checks conflicts against a fixed ledger end and then against in-flight transactions. We expect this approach to scale to very large active contracts sets.

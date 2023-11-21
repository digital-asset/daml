# Canton

[![CircleCI](https://circleci.com/gh/DACH-NY/canton.svg?style=svg&circle-token=5cf96f68761df465e62cf13b03cf75f4e9f67eb7)](https://circleci.com/gh/DACH-NY/canton)

Canton is a next-generation Daml ledger interoperability protocol that implements Daml's built-in models of
authorization and privacy faithfully.

* By partitioning the global state it solves both the privacy problems and the scaling bottlenecks of platforms such as
  a single Ethereum instance.

* It allows developers to balance auditability requirements with the right to forget, making it well-suited for building
  GDPR-compliant systems.

* Canton handles authentication and data transport through our so-called synchronization domains.

* Domains can be deployed at will to address scalability, operational or trust concerns.

* Domains can be implemented on top of various technologies, depending on the trust requirements.

* Domains are permissioned but can be federated at no interoperability cost, yielding a virtual global ledger that
  enables truly global workflow composition.

Refer to the [Canton Whitepaper](https://www.canton.io/publications/canton-whitepaper.pdf) for further details.

## Running

Please read [Getting Started](https://docs.daml.com/canton/tutorials/getting_started.html)
for instructions on how to get started with Canton.

Consult the [Canton User Manual](https://docs.daml.com/canton/about.html) for further
references of Canton's configuration, command-line arguments, or its console.

## Development

Please read our [CONTRIBUTING guidelines](CONTRIBUTING.md).

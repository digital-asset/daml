# Json Api Example

This folder contains scripts and configs demonstrating how to interact with the Ledger using Json API.

It uses curl and websocat to communicate with JSON API.

## Prerequisites

  - Bash-compatible terminal
  - Daml (https://docs.daml.com/getting-started/installation.html)
  - curl (https://github.com/curl/curl)
  - jq (https://github.com/jqlang/jq)
  - (optional) websocat (https://github.com/vi/websocat)


## Running

Use two terminal windows.

In the first window start `./run.sh`
In the second window run `./scenario.sh`


The `scenario.sh` scripts show how to create and exercise transactions and query for active contracts.
It can be run multiple times.

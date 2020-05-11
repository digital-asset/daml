#!/bin/bash

cd /Users/builder/daml/infra/macos/2-vagrant-files

# Assumes physical host has one VSTS node and take physical host name
GUEST_NAME=$(HOSTNAME) VSTS_TOKEN=<VSTS-PAT-token-with-agent-right> vagrant up

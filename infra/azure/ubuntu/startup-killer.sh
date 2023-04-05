#!/bin/sh

sudo apt-get update --y && sudo apt-get upgrade --y

#sudo apt-get install azure-cli

curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

az login --identity

az group create --name test --location eastus
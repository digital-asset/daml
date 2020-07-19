#!/bin/bash

cd daml/infra/macos/1-create-box

sudo macinbox --box-format vmware_desktop --disk 250 --memory 57344 --cpu 10 --user-script user-script.sh


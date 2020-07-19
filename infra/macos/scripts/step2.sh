#!/bin/bash
  
cd ~/daml/infra/macos/2-common-box

GUEST_NAME=$(HOSTNAME) vagrant up

vagrant package --output ~/images/initialized-$(date +%Y%m%d).box

vagrant destroy -f

cd ~
./copyfile.sh images/initialized-$(date +%Y%m%d).box


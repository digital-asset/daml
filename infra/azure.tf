# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  azure-admin-login = "adminuser"
  azure-pub-key     = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCygLXwXmrkMYpxLtyolmOqodZ6w6DZINDLrCnpEpyykWbUdEmbTVYclF92dRLTd84TyQO5lfL7eAUAi6KWYE0DNIXV/Jl93iM+80/i3QqIMjcydzkkkSNJHDPECJoVx+ftm0tCOWZXLudsHgHkMu0Vx2R9XvfUB+MY5sU50NV6wwmAAyZezW8l51/vcPeLb5YX7hV8+VjF9zv5f7SxZGsfALYB2CwddwPoO+/xrnj/Vz9jWsO5y4I7ia1tAs3QOabtz9UPfvVxIoDnEAojgVnsb7GvB4SvxraEQHNLwXcRmzCPPyznOhiAdGKd1kpEMnbD7lKQrKdHX2PdVJZB/PF1ekv62HD6ZuzKmrcB7qUpj6qkDCfaPCw1psTefXFjK53Q2LffZVhanOw+Oq7J1Gdo7phl40ipQyHHr/jp0pMmQ7mZhXnbrQE4H4csMoMbzWH9WcJ+qowBUHMb55Ai0WcVho0/w7+FPAiVDyUobxpaZnqBOV+/n/hC9kkkC1bfokP6oFEi6w4m1/g1LlgWLo+ex9H2ebOt9yiUBsWXwWUyrvbtCANpo510Ss9rCj9NS9vu7iH3GV9JcpaGs1AF7NXNduwI+LCiYK+smBo0T1I8Sq/TpoYtDCAhoGZth4sppetEgMNOFsri7ZZiu0NiJLpEhhVou06CMM/KSwBU2PzeSw== Azure Self Hosted Runners"
}

resource "azurerm_virtual_network" "ubuntu" {
  name                = "ubuntu"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
  address_space       = ["10.0.0.0/16"]

  subnet {
    name           = "subnet"
    address_prefix = "10.0.1.0/24"
    security_group = azurerm_network_security_group.ubuntu.id
  }
}

resource "azurerm_nat_gateway" "nat" {
  name                = "nat"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
}

resource "azurerm_public_ip_prefix" "nat" {
  name                = "nat-ip-prefix"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
  prefix_length       = 28
}

resource "azurerm_nat_gateway_public_ip_prefix_association" "nat" {
  nat_gateway_id      = azurerm_nat_gateway.nat.id
  public_ip_prefix_id = azurerm_public_ip_prefix.nat.id
}

resource "azurerm_subnet_nat_gateway_association" "nat" {
  subnet_id      = one(azurerm_virtual_network.ubuntu.subnet).id
  nat_gateway_id = azurerm_nat_gateway.nat.id
}

resource "azurerm_network_security_group" "ubuntu" {
  name                = "ubuntu"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name

  security_rule {
    name                       = "deny-inbound"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_linux_virtual_machine" "daily-reset" {
  name                  = "daily-reset"
  location              = azurerm_resource_group.daml-ci.location
  resource_group_name   = azurerm_resource_group.daml-ci.name
  network_interface_ids = [azurerm_network_interface.daily-reset.id]
  size                  = "Standard_DS1_v2"

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = "30"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  custom_data = base64encode(<<STARTUP
#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get upgrade -y
apt-get install -y jq
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

echo "$(date -Is -u) boot" > /root/log

az login --identity > /root/log

az configure --defaults group=${azurerm_resource_group.daml-ci.name} > /root/log

cat <<'CRON' > /root/daily-reset.sh
#!/usr/bin/env bash
set -euo pipefail

echo "$(date -Is -u) start"

echo "$(date -Is -u) arg: $1"

month=$(date +%m)
day_of_month=$(date +%d)
if [[ "$month" -eq 12 && "$day_of_month" -gt 22 ]]; then
  # We treat the days after December 22nd as weekend days.
  target='low'
else
  target="$1"
fi

case $target in
  high)
    target='{"du1":10,"du2":0,"dw1":5,"dw2":0}'
    ;;
  low)
    target='{"du1":0,"du2":2,"dw1":0,"dw2":1}'
    ;;
  *)
    echo "ERROR: unexpected target '$target'" >&2
    ;;
esac

echo "$(date -Is -u) target: $target"

AZURE_PAT=${secret_resource.vsts-token.value}

agent_is_busy() (
  pool_id=$1
  agent_id=$2
  curl --silent \
       --fail \
       -u :$AZURE_PAT \
       "https://dev.azure.com/digitalasset/_apis/distributedtask/pools/$pool_id/agents/$agent_id/?includeAssignedRequest=true&includeLastCompletedRequest=true&api-version=5.1" \
       | jq -e .assignedRequest.requestId \
   >/dev/null
)

disable_agent() (
  pool_id=$1
  agent_id=$2
  curl --silent \
       --fail \
       -u :$AZURE_PAT \
       "https://dev.azure.com/digitalasset/_apis/distributedtask/pools/$pool_id/agents/$agent_id?api-version=5.0" \
       -X 'PATCH' \
       -H 'Content-Type: application/json' \
       --data-binary '{"id":'$agent_id',"enabled":false}' \
   >/dev/null
)

remove_agent() (
  pool_id=$1
  agent_id=$2
  curl --silent \
       --fail \
       -u :$AZURE_PAT \
       -XDELETE \
       "https://dev.azure.com/digitalasset/_apis/distributedtask/pools/$pool_id/agents/$agent_id?api-version=7.0" \
   >/dev/null
)

for platform in '{"name":"u","pool":18}' '{"name":"w","pool":11}'; do
  platform_name=$(echo "$platform" | jq -r .name)
  pool_id=$(echo "$platform" | jq -r .pool)
  agents=$(curl --silent \
                --fail \
                -u :$AZURE_PAT \
                "https://dev.azure.com/digitalasset/_apis/distributedtask/pools/$pool_id/agents?api-version=7.0" \
           | jq -c '[.value[] | {name,id}]')
  for scale_set in d$${platform_name}1 d$${platform_name}2; do
  (
    echo "$(date -Is -u) $scale_set: shutting down all machines"
    for idx in $(seq 0 $(echo "$agents" | jq 'length - 1')); do
    (
      agent_name=$(echo "$agents" | jq -r ".[$idx].name")
      agent_id=$(echo "$agents" | jq -r ".[$idx].id")
      if [[ "$${agent_name%-*}" = "$scale_set" ]]; then
        echo "$(date -Is -u) > $agent_name: disabling agent and waiting for job to finish."
        disable_agent $pool_id $agent_id
        while agent_is_busy $pool_id $agent_id; do
          echo "$(date -Is -u) > $agent_name: waiting for jobs to finish."
          sleep 30
        done
        echo "$(date -Is -u) > $agent_name: removing from Azure Pipelines."
        remove_agent $pool_id $agent_id
        echo "$(date -Is -u) > $agent_name: shutting down."
        instance_id=$((36#$${agent_name#*-}))
        az vmss delete-instances --resource-group daml-ci --name $scale_set --instance-ids $instance_id
      fi
    )
    done
    echo "$(date -Is -u) $scale_set: ensuring all machines are gone."
    az vmss scale --resource-group daml-ci --name $scale_set --new-capacity 0 >/dev/null
    echo "$(date -Is -u) $scale_set: create new machines."
    target_size=$(echo "$target" | jq --arg name $scale_set -r '.[$name]')
    az vmss scale --resource-group daml-ci --name $scale_set --new-capacity $target_size >/dev/null
    echo "$(date -Is -u) $scale_set: done."
  )
  done
done
echo "$(date -Is -u) end"
CRON

chmod +x /root/daily-reset.sh

cat <<'CRONTAB' >> /etc/crontab
30 5 * * 1-5 root /root/daily-reset.sh high >> /root/log 2>&1
30 18 * * 1-5 root /root/daily-reset.sh low >> /root/log 2>&1
30 5 * * 6,7 root /root/daily-reset.sh low >> /root/log 2>&1
CRONTAB

tail -f /root/log

STARTUP
  )

  computer_name                   = "daily-reset"
  admin_username                  = local.azure-admin-login
  disable_password_authentication = true

  admin_ssh_key {
    username   = local.azure-admin-login
    public_key = local.azure-pub-key
  }
  identity {
    type = "SystemAssigned"
  }

  # required to get console output in Azure UI
  boot_diagnostics {
    storage_account_uri = null
  }
}

resource "azurerm_network_interface" "daily-reset" {
  name                = "daily-reset"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = one(azurerm_virtual_network.ubuntu.subnet).id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_role_definition" "daily-reset" {
  name  = "daily-reset"
  scope = azurerm_resource_group.daml-ci.id

  permissions {
    actions = [
      "Microsoft.Compute/virtualMachineScaleSets/delete/action",
      "Microsoft.Compute/virtualMachineScaleSets/read",
      "Microsoft.Compute/virtualMachineScaleSets/write",
    ]
  }
}

resource "azurerm_role_assignment" "daily-reset" {
  scope              = azurerm_resource_group.daml-ci.id
  role_definition_id = azurerm_role_definition.daily-reset.role_definition_resource_id
  principal_id       = azurerm_linux_virtual_machine.daily-reset.identity[0].principal_id
}

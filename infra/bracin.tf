# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "azurerm_virtual_network" "bracin" {
  name                = "bracin"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
  address_space       = ["10.0.0.0/16"]

  subnet {
    name           = "subnet"
    address_prefix = "10.0.0.0/24"
    security_group = azurerm_network_security_group.bracin.id
  }

}

resource "azurerm_nat_gateway" "bracin" {
  name                = "bracin"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
}

resource "azurerm_public_ip_prefix" "bracin" {
  name                = "bracin-ip-prefix"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
  prefix_length       = 28
}

resource "azurerm_nat_gateway_public_ip_prefix_association" "bracin" {
  nat_gateway_id      = azurerm_nat_gateway.bracin.id
  public_ip_prefix_id = azurerm_public_ip_prefix.bracin.id
}

resource "azurerm_subnet_nat_gateway_association" "bracin" {
  subnet_id      = one(azurerm_virtual_network.bracin.subnet).id
  nat_gateway_id = azurerm_nat_gateway.bracin.id
}

resource "azurerm_network_security_group" "bracin" {
  name                = "bracin"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name

  security_rule {
    name                       = "deny-inbound"
    priority                   = 102
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                   = "allow-http-from-vpn"
    priority               = 100
    direction              = "Inbound"
    access                 = "Allow"
    protocol               = "Tcp"
    source_port_range      = "*"
    destination_port_range = "3000"
    source_address_prefixes = [
      "35.194.81.56/32",  # North Virginia
      "35.189.40.124/32", # Sydney
      "35.198.147.95/32", # Frankfurt
    ]
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "allow-internet-for-letsencrypt"
    priority                   = 101
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_linux_virtual_machine" "bracin" {
  name                  = "bracin"
  location              = azurerm_resource_group.daml-ci.location
  resource_group_name   = azurerm_resource_group.daml-ci.name
  network_interface_ids = [azurerm_network_interface.bracin.id]
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

groupadd --gid 3000 nonroot
useradd \
  --create-home \
  --gid 3000 \
  --shell /bin/bash \
  --uid 3000 \
  nonroot

mkdir /nix
chown nonroot:nonroot /nix

su --login nonroot <<'NONROOT'
set -euo pipefail

cd $HOME

sh <(curl -L https://nixos.org/nix/install) --no-daemon

mkdir bracin
echo "${filebase64("${path.module}/bracin/project.clj")}" | base64 -d > bracin/project.clj
echo "${filebase64("${path.module}/bracin/shell.nix")}" | base64 -d > bracin/shell.nix
mkdir bracin/nix
echo "${filebase64("${path.module}/bracin/nix/nixpkgs.nix")}" | base64 -d > bracin/nix/nixpkgs.nix
echo "${filebase64("${path.module}/bracin/nix/src.json")}" | base64 -d > bracin/nix/src.json
mkdir -p bracin/src/bracin
echo "${filebase64("${path.module}/bracin/src/bracin/core.clj")}" | base64 -d > bracin/src/bracin/core.clj

cd bracin
$HOME/.nix-profile/bin/nix-shell --pure --run 'lein run'
NONROOT
STARTUP
  )

  computer_name                   = "bracin"
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

resource "azurerm_network_interface" "bracin" {
  name                = "bracin"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name

  ip_configuration {
    name                          = "public"
    subnet_id                     = one(azurerm_virtual_network.bracin.subnet).id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.bracin.id
  }
}

resource "azurerm_public_ip" "bracin" {
  name                = "bracin"
  resource_group_name = azurerm_resource_group.daml-ci.name
  location            = azurerm_resource_group.daml-ci.location
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_role_definition" "bracin" {
  name  = "bracin"
  scope = azurerm_resource_group.daml-ci.id

  permissions {
    actions = [
      "Microsoft.Compute/virtualMachineScaleSets/delete/action",
      "Microsoft.Compute/virtualMachineScaleSets/read",
      "Microsoft.Compute/virtualMachineScaleSets/write",
    ]
  }
}

resource "azurerm_role_assignment" "bracin" {
  scope              = azurerm_resource_group.daml-ci.id
  role_definition_id = azurerm_role_definition.bracin.role_definition_resource_id
  principal_id       = azurerm_linux_virtual_machine.bracin.identity[0].principal_id
}

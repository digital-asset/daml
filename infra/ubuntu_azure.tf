# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "azurerm_linux_virtual_machine_scale_set" "ubuntu" {
  name                = "ubuntu"
  resource_group_name = azurerm_resource_group.daml-ci.name
  location            = azurerm_resource_group.daml-ci.location
  sku                 = "Standard_DS1_v2"
  instances           = 1

  admin_username                  = "adminuser"
  disable_password_authentication = true
  admin_ssh_key {
    username   = "adminuser"
    public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC/8O+yV3IEo2I+6bzkj2WnyLxvayOVP4FUZU6HOzOMKQr7XcD3EL74qngoJFGgrdqIPi6h7JP1+dN/0wFPF3V08PjMO+Ql1j0jOc8y4u8Q0lv0LZprzYi71lFzhq9XMP1jSLc+hKxcYbceJqHWfcB5ek5tGJIkBKhgOGlOH36MsvhYBvcXmq5Qm6OCTzy4jq2b6QKYv0jZAYBJEgCtIA4ULHI8HI31VrWcra9YuOMNwy+e6Li2CNbY7W8emA4oU9JC0pt/pSbxyr4OZYHz+ZrdTAX6o9ZHAi3ghDBAEPdv1g4uZsKa2g7751FjzyTODfLEzWHr8aGM/ibiTnhpFkdyo7y2EvUfvO/taXVm3d3MIiC3R4tMkNcOPBj3nXsZWE1fd3fedj+W0W6eRvSgJuWc64zyWE+/XniPqGrexF9y17s2+Br6Es0wY0DZbcUUDf93fjv6FBl2IN6o5zSVjcL0EH4DsuFi2epMNupr3I4+iInTpGZ4FIHqH5LRYShpzSU= gary@garyverhaegenxp02j7"
  }

  computer_name_prefix = "daml-ubuntu"

  # save a bit of energy for the planet
  overprovision = false

  #  custom_data = base64encode(templatefile("startup.sh", {
  #    vsts_token   = secret_resource.vsts-token.value
  #    vsts_account = "digitalasset"
  #    vsts_pool    = "ubuntu_20_04"
  #    size         = 400
  #  }))
  custom_data = base64encode(<<EOF
#!/usr/bin/env bash

echo "Hello!"

EOF
  )

  source_image_reference {
    publisher = "canonical"
    offer     = "0001-com-ubuntu-server-focal"
    sku       = "20_04-lts"
    version   = "latest"
  }

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Standard_LRS"
  }

  data_disk {
    storage_account_type = "StandardSSD_LRS"
    create_option        = "Empty"
    caching              = "ReadWrite"
    lun                  = 0
    disk_size_gb         = 400
  }

  network_interface {
    name        = "default"
    primary     = true
    dns_servers = ["10.0.0.4", "10.0.0.5"]

    ip_configuration {
      name      = "default"
      primary   = true
      subnet_id = one(azurerm_virtual_network.ubuntu.subnet).id
    }
  }

  # required to get console output in Azure UI
  boot_diagnostics {
    storage_account_uri = null
  }

}

resource "azurerm_virtual_network" "ubuntu" {
  name                = "ubuntu"
  location            = azurerm_resource_group.daml-ci.location
  resource_group_name = azurerm_resource_group.daml-ci.name
  address_space       = ["10.0.0.0/16"]
  dns_servers         = ["10.0.0.4", "10.0.0.5"]

  subnet {
    name           = "subnet"
    address_prefix = "10.0.1.0/24"
    security_group = azurerm_network_security_group.ubuntu.id
  }
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

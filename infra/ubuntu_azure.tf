# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "azurerm_linux_virtual_machine_scale_set" "ubuntu" {
  count               = length(local.ubuntu.azure)
  name                = "ubuntu"
  resource_group_name = azurerm_resource_group.daml-ci.name
  location            = azurerm_resource_group.daml-ci.location
  sku                 = "Standard_D4_v2"
  instances           = local.ubuntu.azure[count.index].size

  admin_username                  = "adminuser"
  disable_password_authentication = true
  admin_ssh_key {
    username   = "adminuser"
    public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCygLXwXmrkMYpxLtyolmOqodZ6w6DZINDLrCnpEpyykWbUdEmbTVYclF92dRLTd84TyQO5lfL7eAUAi6KWYE0DNIXV/Jl93iM+80/i3QqIMjcydzkkkSNJHDPECJoVx+ftm0tCOWZXLudsHgHkMu0Vx2R9XvfUB+MY5sU50NV6wwmAAyZezW8l51/vcPeLb5YX7hV8+VjF9zv5f7SxZGsfALYB2CwddwPoO+/xrnj/Vz9jWsO5y4I7ia1tAs3QOabtz9UPfvVxIoDnEAojgVnsb7GvB4SvxraEQHNLwXcRmzCPPyznOhiAdGKd1kpEMnbD7lKQrKdHX2PdVJZB/PF1ekv62HD6ZuzKmrcB7qUpj6qkDCfaPCw1psTefXFjK53Q2LffZVhanOw+Oq7J1Gdo7phl40ipQyHHr/jp0pMmQ7mZhXnbrQE4H4csMoMbzWH9WcJ+qowBUHMb55Ai0WcVho0/w7+FPAiVDyUobxpaZnqBOV+/n/hC9kkkC1bfokP6oFEi6w4m1/g1LlgWLo+ex9H2ebOt9yiUBsWXwWUyrvbtCANpo510Ss9rCj9NS9vu7iH3GV9JcpaGs1AF7NXNduwI+LCiYK+smBo0T1I8Sq/TpoYtDCAhoGZth4sppetEgMNOFsri7ZZiu0NiJLpEhhVou06CMM/KSwBU2PzeSw== Azure Self Hosted Runners"
  }

  computer_name_prefix = "daml-ubuntu"

  # save a bit of energy for the planet
  overprovision = false

  custom_data = base64encode(templatefile("${path.module}/ubuntu_startup.sh", {
    vsts_token   = secret_resource.vsts-token.value
    vsts_account = "digitalasset"
    vsts_pool    = "ubuntu_20_04"
    size         = local.ubuntu.azure[count.index].disk_size
    gcp_logging  = ""
    assignment   = local.ubuntu.azure[count.index].assignment
  }))

  source_image_reference {
    publisher = "canonical"
    offer     = "0001-com-ubuntu-server-focal"
    sku       = "20_04-lts"
    version   = "latest"
  }

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = local.ubuntu.azure[count.index].disk_size
  }

  network_interface {
    name    = "default"
    primary = true

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

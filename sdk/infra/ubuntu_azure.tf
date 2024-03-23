# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "azurerm_linux_virtual_machine_scale_set" "ubuntu" {
  count               = length(local.ubuntu.azure)
  name                = local.ubuntu.azure[count.index].name
  resource_group_name = azurerm_resource_group.daml-ci.name
  location            = azurerm_resource_group.daml-ci.location
  sku                 = "Standard_D8_v5"
  instances           = local.ubuntu.azure[count.index].size

  admin_username                  = local.azure-admin-login
  disable_password_authentication = true
  admin_ssh_key {
    username   = local.azure-admin-login
    public_key = local.azure-pub-key
  }

  computer_name_prefix = "${local.ubuntu.azure[count.index].name}-"

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
    caching              = "ReadWrite"
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

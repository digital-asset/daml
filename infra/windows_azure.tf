# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "random_password" "windows_admin" {
  length  = 123
  special = true
}

resource "azurerm_windows_virtual_machine_scale_set" "deployment" {
  count               = length(local.windows.azure)
  name                = local.windows.azure[count.index].name
  resource_group_name = azurerm_resource_group.daml-ci.name
  location            = azurerm_resource_group.daml-ci.location
  sku                 = "Standard_D4_v2"
  instances           = local.windows.azure[count.index].size

  admin_username = local.azure-admin-login
  admin_password = random_password.windows_admin.result

  computer_name_prefix = "${local.windows.azure[count.index].name}-"

  # save a bit of energy for the planet
  overprovision = false

  custom_data = base64encode(templatefile("${path.module}/windows_startup.ps1", {
    vsts_token   = secret_resource.vsts-token.value
    vsts_account = "digitalasset"
    vsts_pool    = "windows-pool"
    gcp_logging  = ""
    assignment   = local.windows.azure[count.index].assignment
  }))

  source_image_reference {
    publisher = "MicrosoftWindowsServer"
    offer     = "WindowsServer"
    sku       = "2016-datacenter"
    version   = "latest"
  }

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = local.windows.azure[count.index].disk_size

  }

  data_disk {
    storage_account_type = "StandardSSD_LRS"
    create_option        = "Empty"
    caching              = "ReadWrite"
    lun                  = 0
    disk_size_gb         = local.windows.azure[count.index].disk_size
  }

  network_interface {
    name    = "external"
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

  extension {
    name                 = "startup"
    publisher            = "Microsoft.Compute"
    type                 = "CustomScriptExtension"
    type_handler_version = "1.10"
    settings = jsonencode(
      {
        # Nothing's ever easy on Windows :(
        commandToExecute = "powershell -ExecutionPolicy Unrestricted -Command Copy-Item -Path C:/AzureData/CustomData.bin C:/AzureData/CustomData.ps1; powershell -ExecutionPolicy Unrestricted -File C:/AzureData/CustomData.ps1"
    })
  }

  /*
  identity {
    type = "SystemAssigned"
  }
  */

  #Currently this extenstion is only available for domain joined windows machines.
  /*  extension {
    name                       = "AADLoginForWindows"
    publisher                  = "Microsoft.Azure.ActiveDirectory"
    type                       = "AADLoginForWindows"
    type_handler_version       = "1.0"
    auto_upgrade_minor_version = true
  }*/

  /*
  extension {
    name                       = "Custom-Startup-Script"
    publisher                  = "Microsoft.Compute"
    type                       = "CustomScriptExtension"
    type_handler_version       = "1.10"
    auto_upgrade_minor_version = true

    protected_settings = <<PROTECTED_SETTINGS
    {
      "fileUris": [
          "https://github.com/asangeethada/daml/blob/infra-add-azure/infra/azure/windows/startup.ps1"
        ],
      "commandToExecute": "powershell.exe -ExecutionPolicy Unrestricted -Command \"./startup.ps1 -vsts_token \"${secret_resource.vsts-token.value}\"; exit 0;\""
    }
  PROTECTED_SETTINGS

  }
  */
}

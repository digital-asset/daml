resource "secret_resource" "vsts-token" {}
resource "random_pet" "rg_name" {
  prefix = var.resource_group_name_prefix
}

resource "azurerm_resource_group" "rg" {
  location = var.resource_group_location
  name     = random_pet.rg_name.id
}

resource "azurerm_virtual_network" "my_terraform_network" {
  name                = "myVnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

# Create subnet
resource "azurerm_subnet" "my_terraform_subnet" {
  name                 = "mySubnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.my_terraform_network.name
  address_prefixes     = ["10.0.1.0/24"]
}


#Create KeyVault ID
resource "random_id" "kvname" {
  byte_length = 5
  prefix      = "keyvault"
  #resource_group_name = azurerm_resource_group.rg.name
}

#Keyvault Creation
data "azuread_client_config" "current" {} #needed because of bug in azurerm_client_config
data "azurerm_client_config" "current" {}
resource "azurerm_key_vault" "kv1" {
  depends_on                  = [azurerm_resource_group.rg]
  name                        = random_id.kvname.hex
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  #soft_delete_retention_days  = 7
  purge_protection_enabled = false
  sku_name                 = "standard"
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azuread_client_config.current.object_id

    key_permissions = [
      "Get",
    ]

    secret_permissions = [
      "Get", "Backup", "Delete", "List", "Purge", "Recover", "Restore", "Set",
    ]

    storage_permissions = [
      "Get",
    ]
  }
}

#Create KeyVault VM password
resource "random_password" "vmpassword" {
  length  = 20
  special = true
}
#Create Key Vault Secret
resource "azurerm_key_vault_secret" "vmpassword" {
  name         = "vmpassword"
  value        = random_password.vmpassword.result
  key_vault_id = azurerm_key_vault.kv1.id
  depends_on   = [azurerm_key_vault.kv1]
  #resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_public_ip_prefix" "pubip_prefix" {
  name                = "ip-prefix-vmss"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  prefix_length       = 30
}

resource "azurerm_network_security_group" "my_terraform_nsg" {
  name                = "myNetworkSecurityGroup"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  security_rule {
    name                       = "RDP"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "3389"
    source_address_prefixes    = ["35.198.147.95/32", "35.194.81.56/32"]
    destination_address_prefix = "*"
  }



  security_rule {
    name                       = "Deny_subnet_traffic"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = azurerm_subnet.my_terraform_subnet.address_prefixes[0]
    destination_address_prefix = azurerm_subnet.my_terraform_subnet.address_prefixes[0]
  }

}


resource "azurerm_windows_virtual_machine_scale_set" "deployment" {
  name                = "c1-w1"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  //upgrade_policy_mode = "Manual"
  instances              = 1 #instance count to begin with
  overprovision          = false
  single_placement_group = false
  sku                    = "Standard_DS1_v2" #Standard_F16s_v2 to match current setup on GCP


  admin_username = "adminuser"
  admin_password = azurerm_key_vault_secret.vmpassword.value //azurerm_key_vault_secret.vmsecret.value


  #disable_password_authentication = true




  source_image_reference {
    publisher = "MicrosoftWindowsServer"
    offer     = "WindowsServer"
    sku       = "2019-datacenter-gensecond"
    version   = "latest"
  }

  os_disk {
    storage_account_type = "StandardSSD_LRS"
    caching              = "ReadOnly"
    disk_size_gb         = 256

  }
  /*data_disk {
    storage_account_type = "StandardSSD_LRS"
    create_option       = "Empty"
    caching              = "ReadWrite"
    lun                  = 0
    disk_size_gb         = 400
  }*/

  network_interface {
    name        = "external"
    primary     = true
    dns_servers = []

    ip_configuration {
      application_gateway_backend_address_pool_ids = []
      application_security_group_ids               = []
      load_balancer_backend_address_pool_ids       = []
      load_balancer_inbound_nat_rules_ids          = []
      name                                         = "external"
      primary                                      = true
      subnet_id                                    = azurerm_subnet.my_terraform_subnet.id

      public_ip_address {
        name                = "public-ip-address"
        public_ip_prefix_id = azurerm_public_ip_prefix.pubip_prefix.id
      }

    }


    network_security_group_id = azurerm_network_security_group.my_terraform_nsg.id
  }

  boot_diagnostics {
    storage_account_uri = null
  }

  identity {
    type = "SystemAssigned"
  }

#Currently this extenstion is only available for domain joined windows machines.
/*  extension {
    name                       = "AADLoginForWindows"
    publisher                  = "Microsoft.Azure.ActiveDirectory"
    type                       = "AADLoginForWindows"
    type_handler_version       = "1.0"
    auto_upgrade_minor_version = true
  }*/

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
    tags = var.tags
}

resource "azurerm_monitor_autoscale_setting" "example" {
  name                = "myAutoscaleSetting"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  target_resource_id  = azurerm_windows_virtual_machine_scale_set.deployment.id

  profile {
    name = "Test"

    capacity {
      default = 1
      minimum = 1
      maximum = 2
    }
  }
}


#resource "secret_resource" "vsts-token" {}

# Data template Bash bootstrapping file
data "local_file" "startup" {
  filename = "${path.module}/Startup.ps1"
}

terraform {
  required_version = ">=0.12"

  required_providers {
    secret = {
      source  = "numtide/secret"
      version = "1.2.1"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~>3.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~>4.0"
    }
     azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.15.0"
    }
  }

}

provider "azurerm" {
  features {}
}

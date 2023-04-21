# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

terraform {
  backend "gcs" {
    bucket = "da-dev-gcp-daml-language-tfstate"
    prefix = "daml"
  }

  required_providers {
    secret = {
      source  = "numtide/secret"
      version = "1.2.1"
    }
    google = {
      source  = "hashicorp/google"
      version = "4.43.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "4.43.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.31.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.4.3"
    }
  }
}

provider "google" {
  project = "da-dev-gcp-daml-language"
  region  = local.region
  zone    = local.zone
}

provider "google-beta" {
  project = "da-dev-gcp-daml-language"
  region  = local.region
  zone    = local.zone
}

provider "secret" {
}

provider "azurerm" {
  features {
    virtual_machine {
      graceful_shutdown          = true
      delete_os_disk_on_deletion = true
    }
  }
  subscription_id = "171d5caa-4ddb-4890-8635-919d35f489e0"
}

resource "azurerm_resource_group" "daml-ci" {
  name     = "daml-ci"
  location = "East US"
}

data "google_project" "current" {
  project_id = local.project
}

locals {
  labels = {
    cost-allocation = "daml-language"
    host-group      = "buildpipeline"
    infra-owner     = "daml-language"
    managed         = "true"
    target          = "infra"
  }

  machine-labels = merge(local.labels, tomap({ "env" = "production" }))

  project = "da-dev-gcp-daml-language"
  region  = "us-east4"
  zone    = "us-east4-a"

  ssl_certificate_hoogle = "https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/hoogle-google-cert"
}

resource "secret_resource" "vsts-token" {}

resource "google_compute_ssl_policy" "ssl_policy" {
  name            = "ssl-policy"
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"
}

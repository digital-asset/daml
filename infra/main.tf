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
      version = "1.2.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "4.9.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "4.9.0"
    }
    template = {
      source  = "hashicorp/template"
      version = "2.2.0"
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

provider "template" {
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

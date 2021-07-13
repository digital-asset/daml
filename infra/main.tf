# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

terraform {
  backend "gcs" {
    bucket = "da-dev-gcp-daml-language-tfstate"
    prefix = "daml"
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
  version = "~>1.1"
}

provider "template" {
  version = "~>2.2"
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

  machine-labels = merge(local.labels, map("env", "production"))

  project = "da-dev-gcp-daml-language"
  region  = "us-east4"
  zone    = "us-east4-a"

  ssl_certificate_hoogle = "https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/hoogle-google-cert"
}

resource "secret_resource" "vsts-token" {}

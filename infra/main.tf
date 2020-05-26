# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

terraform {
  backend "gcs" {
    bucket = "da-dev-gcp-daml-language-tfstate"
    prefix = "daml"
  }
}

provider "google" {
  project = "da-dev-gcp-daml-language"
  region  = "us-east4"
  version = "3.5"
}

provider "google-beta" {
  project = "da-dev-gcp-daml-language"
  region  = "us-east4"
  version = "3.5"
}

provider "secret" {
  version = "1.1"
}

provider "template" {
  version = "2.1.2"
}

data "google_project" "current" {
  project_id = "${local.project}"
}

locals {
  labels = {
    cost-allocation = "daml-language"
    host-group      = "buildpipeline"
    infra-owner     = "daml-language"
    managed         = "true"

    # default the target name to be the name of the folder
    target = "${basename(path.module)}"
  }

  project = "da-dev-gcp-daml-language"
  region  = "us-east4"
  zone    = "us-east4-a"

  // maintained by DA security
  ssl_certificate = "https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/da-ext-wildcard"

  ssl_certificate_hoogle = "https://www.googleapis.com/compute/v1/projects/da-dev-gcp-daml-language/global/sslCertificates/daml-lang-hoogle-app-service-https-cert"
}

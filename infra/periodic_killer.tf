# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# This file defines a machine meant to destroy/recreate all our CI nodes every
# night.

resource "google_service_account" "periodic-killer" {
  account_id = "periodic-killer"
}

resource "google_project_iam_custom_role" "tf-write-state" {
  role_id = "killCiNodesEveryNight"
  title   = "Permissions to list & kill CI nodes every night"
  permissions = [
    "compute.zones.list",
  ]
}

resource "google_compute_instance" "periodic-killer" {
  name         = "periodic-killer"
  machine_type = "n1-standard-1"
  zone         = "us-east4-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-1804-lts"
    }
  }

  network_interface {
    network = "default"

    // Ephemeral IP to get access to the Internet
    access_config {}
  }

  service_account {
    email  = "${google_service_account.periodic-killer.email}"
    scopes = ["cloud-platform"]
  }
  allow_stopping_for_update = true

  metadata_startup_script = <<STARTUP
set -euxo pipefail

apt-get update
apt-get install -y curl jq

curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-270.0.0-linux-x86_64.tar.gz | tar xz
export PATH="$(pwd)/google-cloud-sdk/bin:$PATH"

STARTUP
}

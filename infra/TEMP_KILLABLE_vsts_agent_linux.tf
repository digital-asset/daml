# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_region_instance_group_manager" "temp-killable" {
  provider           = "google-beta"
  name               = "temp-killable"
  base_instance_name = "temp-killable"
  region             = "${local.region}"
  target_size        = 3

  version {
    name              = "temp-killable"
    instance_template = "${google_compute_instance_template.temp-killable.self_link}"
  }

  update_policy {
    type            = "PROACTIVE"
    minimal_action  = "REPLACE"
    max_surge_fixed = 3
    min_ready_sec   = 60
  }
}

resource "google_compute_instance_template" "temp-killable" {
  name_prefix  = "killable-"
  machine_type = "n1-standard-1"
  labels       = "${local.labels}"

  disk {
    disk_size_gb = 20
    disk_type    = "pd-ssd"
    source_image = "ubuntu-os-cloud/ubuntu-1604-lts"
  }

  lifecycle {
    create_before_destroy = true
  }

  network_interface {
    network = "default"

    // Ephemeral IP to get access to the Internet
    access_config {}
  }

  service_account {
    email  = "log-writer@da-dev-gcp-daml-language.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = true
  }
}

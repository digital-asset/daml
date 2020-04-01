# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

data "template_file" "temp-moritz-startup" {
  template = "${file("${path.module}/TEMP_moritz_vsts_agent_linux_startup.sh")}"
}

resource "google_compute_region_instance_group_manager" "temp-moritz-linux" {
  provider           = "google-beta"
  name               = "temp-moritz-linux"
  base_instance_name = "temp-moritz-linux"
  region             = "${local.region}"
  target_size        = 1

  version {
    name              = "temp-moritz-linux"
    instance_template = "${google_compute_instance_template.temp-moritz-linux.self_link}"
  }

  update_policy {
    type            = "PROACTIVE"
    minimal_action  = "REPLACE"
    max_surge_fixed = 3
    min_ready_sec   = 60
  }
}

resource "google_compute_instance_template" "temp-moritz-linux" {
  name_prefix  = "temp-moritz-linux-"
  machine_type = "n1-standard-8"
  labels       = "${local.labels}"

  disk {
    disk_size_gb = 200
    disk_type    = "pd-ssd"
    source_image = "ubuntu-os-cloud/ubuntu-1604-lts"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata {
    startup-script = "${data.template_file.temp-moritz-startup.rendered}"
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
    preemptible         = false
  }
}

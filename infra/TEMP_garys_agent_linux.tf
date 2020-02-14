# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

data "template_file" "TEMP_garys-agent-linux-startup" {
  template = "${file("${path.module}/TEMP_garys_agent_linux_startup.sh")}"

  vars = {
    vsts_token   = "${secret_resource.vsts-token.value}"
    vsts_account = "digitalasset"
    vsts_pool    = "garys-linux-pool"
  }
}

resource "google_compute_region_instance_group_manager" "TEMP_garys-agent-linux" {
  provider           = "google-beta"
  name               = "TEMP_garys-agent-linux"
  base_instance_name = "TEMP_garys-agent-linux"
  region             = "${local.region}"
  target_size        = 1

  version {
    name              = "TEMP_garys-agent-linux"
    instance_template = "${google_compute_instance_template.TEMP_garys-agent-linux.self_link}"
  }

  update_policy {
    type            = "PROACTIVE"
    minimal_action  = "REPLACE"
    max_surge_fixed = 3
    min_ready_sec   = 60
  }
}

resource "google_compute_instance_template" "TEMP_garys-agent-linux" {
  name_prefix  = "TEMP_garys-agent-linux-"
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
    startup-script = "${data.template_file.TEMP_garys-agent-linux-startup.rendered}"

    shutdown-script = <<EOS
#!/usr/bin/env bash
set -euo pipefail
cd /home/vsts/agent
su vsts <<SHUTDOWN_AGENT
export VSTS_AGENT_INPUT_TOKEN='${secret_resource.vsts-token.value}'
./config.sh remove --unattended --auth PAT
SHUTDOWN_AGENT
    EOS
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

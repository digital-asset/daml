# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "secret_resource" "vsts-token" {}

data "template_file" "vsts-agent-linux-startup" {
  template = "${file("${path.module}/vsts_agent_linux_startup.sh")}"

  vars = {
    vsts_token   = "${secret_resource.vsts-token.value}"
    vsts_account = "digitalasset"
    vsts_pool    = "linux-pool"
  }
}

resource "google_compute_region_instance_group_manager" "vsts-agent-linux" {
  provider           = "google-beta"
  name               = "vsts-agent-linux"
  base_instance_name = "vsts-agent-linux"
  region             = "us-east1"
  target_size        = 10

  version {
    name              = "vsts-agent-linux"
    instance_template = "${google_compute_instance_template.vsts-agent-linux.self_link}"
  }

  update_policy {
    type            = "PROACTIVE"
    minimal_action  = "REPLACE"
    max_surge_fixed = 3
    min_ready_sec   = 60
  }
}

resource "google_compute_instance_template" "vsts-agent-linux" {
  name_prefix  = "vsts-agent-linux-"
  machine_type = "c2-standard-8"
  labels       = "${local.machine-labels}"

  disk {
    disk_size_gb = 200
    disk_type    = "pd-ssd"
    source_image = "ubuntu-os-cloud/ubuntu-1604-lts"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata {
    startup-script = "${data.template_file.vsts-agent-linux-startup.rendered}"

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

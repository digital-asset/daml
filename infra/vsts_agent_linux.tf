# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  region             = "${local.region}"
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
  machine_type = "n1-standard-8"
  labels       = "${local.labels}"

  disk {
    disk_size_gb = 100
    disk_type    = "pd-ssd"
    source_image = "debian-cloud/debian-9"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata {
    startup-script = "${data.template_file.vsts-agent-linux-startup.rendered}"

    shutdown-script = <<EOS
#!/usr/bin/env bash
su --login vsts <<SHUTDOWN_AGENT
cd agent
./config.sh remove \
  --unattended \
  --auth PAT \
  --token '${secret_resource.vsts-token.value}'
SHUTDOWN_AGENT
    EOS
  }

  network_interface {
    network = "default"

    // Ephemeral IP to get access to the Internet
    access_config {}
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = true
  }
}

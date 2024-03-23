# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_region_instance_group_manager" "vsts-agent-ubuntu_20_04" {
  count              = length(local.ubuntu.gcp)
  provider           = google-beta
  name               = local.ubuntu.gcp[count.index].name
  base_instance_name = local.ubuntu.gcp[count.index].name
  region             = "us-east1"
  target_size        = local.ubuntu.gcp[count.index].size

  version {
    name              = local.ubuntu.gcp[count.index].name
    instance_template = google_compute_instance_template.vsts-agent-ubuntu_20_04[count.index].self_link
  }

  # uncomment when we get a provider >3.55
  #distribution_policy_target_shape = "ANY"

  update_policy {
    type            = "PROACTIVE"
    minimal_action  = "REPLACE"
    max_surge_fixed = 3
    min_ready_sec   = 60

    instance_redistribution_type = "NONE"
  }
}

resource "google_compute_instance_template" "vsts-agent-ubuntu_20_04" {
  count        = length(local.ubuntu.gcp)
  name_prefix  = "${local.ubuntu.gcp[count.index].name}-"
  machine_type = "c2-standard-8"
  labels       = local.machine-labels

  disk {
    disk_size_gb = local.ubuntu.gcp[count.index].disk_size
    disk_type    = "pd-ssd"
    source_image = "ubuntu-os-cloud/ubuntu-2004-lts"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata = {
    startup-script = templatefile("${path.module}/ubuntu_startup.sh", {
      vsts_token   = secret_resource.vsts-token.value
      vsts_account = "digitalasset"
      vsts_pool    = "ubuntu_20_04"
      size         = local.ubuntu.gcp[count.index].disk_size
      gcp_logging  = <<EOF
# Taken from https://cloud.google.com/logging/docs/agent/logging/installation
curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
curl -sSL https://dl.google.com/cloudagents/add-logging-agent-repo.sh | bash -s -- --also-install
EOF
      assignment   = local.ubuntu.gcp[count.index].assignment
    })

    shutdown-script = nonsensitive("#!/usr/bin/env bash\nset -euo pipefail\ncd /home/vsts/agent\nsu vsts <<SHUTDOWN_AGENT\nexport VSTS_AGENT_INPUT_TOKEN='${secret_resource.vsts-token.value}'\n./config.sh remove --unattended --auth PAT\nSHUTDOWN_AGENT\n    ")
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

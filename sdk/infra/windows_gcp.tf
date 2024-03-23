# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


resource "google_compute_region_instance_group_manager" "vsts-agent-windows" {
  count    = length(local.windows.gcp)
  provider = google-beta
  name     = local.windows.gcp[count.index].name

  # keep the name short. windows hostnames are limited to 12(?) chars.
  # -5 for the random postfix:
  base_instance_name = local.windows.gcp[count.index].name

  region      = "us-east1"
  target_size = local.windows.gcp[count.index].size

  version {
    name              = local.windows.gcp[count.index].name
    instance_template = google_compute_instance_template.vsts-agent-windows[count.index].self_link
  }

  # uncomment when we get a provider >3.55
  #distribution_policy_target_shape = "ANY"

  update_policy {
    type           = "PROACTIVE"
    minimal_action = "REPLACE"

    # minimum is the number of availability zones (3)
    max_surge_fixed = 3

    # calculated with: serial console last timestamp after boot - VM start
    # 09:54:28 - 09:45:55 = 513 seconds
    min_ready_sec = 520

    instance_redistribution_type = "NONE"
  }
}

resource "google_compute_instance_template" "vsts-agent-windows" {
  count        = length(local.windows.gcp)
  name_prefix  = "${local.windows.gcp[count.index].name}-"
  machine_type = "c2-standard-8"
  labels       = local.machine-labels

  disk {
    disk_size_gb = local.windows.gcp[count.index].disk_size
    disk_type    = "pd-ssd"

    # find the image name with `gcloud compute images list`
    source_image = "windows-cloud/windows-2016"
  }

  # Drive D:\ for the agent work folder
  disk {
    disk_size_gb = local.windows.gcp[count.index].disk_size
    disk_type    = "pd-ssd"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata = {
    // Prepare the machine
    windows-startup-script-ps1 = templatefile("${path.module}/windows_startup.ps1", {
      vsts_token   = nonsensitive(secret_resource.vsts-token.value)
      vsts_account = "digitalasset"
      vsts_pool    = "windows-pool"
      gcp_logging  = <<EOF
# Redirect logs to SumoLogic

cd $env:UserProfile;
Invoke-WebRequest https://dl.google.com/cloudagents/windows/StackdriverLogging-v1-9.exe -OutFile StackdriverLogging-v1-9.exe;
.\StackdriverLogging-v1-9.exe /S /D="C:\Stackdriver\Logging\"
EOF
      assignment   = local.windows.gcp[count.index].assignment
      azure_disk   = ""
    })
    windows-shutdown-script-ps1 = nonsensitive("c://agent/config remove --unattended --auth PAT --token '${secret_resource.vsts-token.value}'")
  }

  network_interface {
    network = "default"

    // Ephemeral IP to get access to the Internet
    access_config {}
  }

  service_account {
    scopes = ["cloud-platform"]
    email  = "log-writer@da-dev-gcp-daml-language.iam.gserviceaccount.com"
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = false
  }
}

# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

locals {
  ubuntu = [
    {
      name      = "ci-u1",
      disk_size = 200,
      size      = 30,
      docker    = <<EOF
# BEGIN Installing Docker per https://docs.docker.com/engine/install/ubuntu/
apt-get -y install apt-transport-https \
                   ca-certificates \
                   curl \
                   gnupg \
                   lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get -y install docker-ce docker-ce-cli containerd.io
docker run --rm hello-world
# END Installing Docker
EOF
    },
    {
      name      = "ci-u2",
      disk_size = 400,
      size      = 0,
      docker    = <<EOF
DOCKER_VERSION="5:20.10.2~3-0~ubuntu-$(lsb_release -cs)"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -qy docker-ce=$DOCKER_VERSION docker-ce-cli=$DOCKER_VERSION containerd.io
EOF
    },
  ]
}

data "template_file" "vsts-agent-ubuntu_20_04-startup" {
  count    = length(local.ubuntu)
  template = file("${path.module}/vsts_agent_ubuntu_20_04_startup.sh")

  vars = {
    vsts_token   = secret_resource.vsts-token.value
    vsts_account = "digitalasset"
    vsts_pool    = "ubuntu_20_04"
    docker       = local.ubuntu[count.index].docker
  }
}

resource "google_compute_region_instance_group_manager" "vsts-agent-ubuntu_20_04" {
  count              = length(local.ubuntu)
  provider           = google-beta
  name               = local.ubuntu[count.index].name
  base_instance_name = local.ubuntu[count.index].name
  region             = "us-east1"
  target_size        = local.ubuntu[count.index].size

  version {
    name              = local.ubuntu[count.index].name
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
  count        = length(local.ubuntu)
  name_prefix  = "${local.ubuntu[count.index].name}-"
  machine_type = "c2-standard-8"
  labels       = local.machine-labels

  disk {
    disk_size_gb = local.ubuntu[count.index].disk_size
    disk_type    = "pd-ssd"
    source_image = "ubuntu-os-cloud/ubuntu-2004-lts"
  }

  lifecycle {
    create_before_destroy = true
  }

  metadata = {
    startup-script = data.template_file.vsts-agent-ubuntu_20_04-startup[count.index].rendered

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

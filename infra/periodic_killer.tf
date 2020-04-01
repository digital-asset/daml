# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# This file defines a machine meant to destroy/recreate all our CI nodes every
# night.

resource "google_service_account" "periodic-killer" {
  account_id = "periodic-killer"
}

resource "google_project_iam_member" "periodic-killer" {
  # should reference google_project_iam_custom_role.periodic-killer.id or
  # something, but for whatever reason that's not exposed.
  role   = "projects/da-dev-gcp-daml-language/roles/killCiNodesEveryNight"
  member = "serviceAccount:${google_service_account.periodic-killer.email}"
}

resource "google_project_iam_custom_role" "periodic-killer" {
  role_id = "killCiNodesEveryNight"
  title   = "Permissions to list & kill CI nodes every night"
  permissions = [
    "compute.instances.delete",
    "compute.instances.list",
    "compute.zones.list",
  ]
}

resource "google_compute_instance" "periodic-killer" {
  name         = "periodic-killer"
  machine_type = "f1-micro"
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
apt-get install -y jq

cat <<CRON > /root/periodic-kill.sh
#!/usr/bin/env bash
set -euo pipefail

PREFIX=vsts-
MACHINES=\$(/snap/bin/gcloud compute instances list --format=json | jq -c '.[] | select(.name | startswith("'\$PREFIX'")) | [.name, .zone]')

for m in \$MACHINES; do
    /snap/bin/gcloud -q compute instances delete \$(echo \$m | jq -r '.[0]') --zone=\$(echo \$m | jq -r '.[1]')
done
CRON

chmod +x /root/periodic-kill.sh

cat <<CRONTAB >> /etc/crontab
0 4 * * * root /root/periodic-kill.sh
CRONTAB

STARTUP
}

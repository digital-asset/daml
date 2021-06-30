# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_network" "es" {
  name = "es-network"
}

locals {
  es_clusters = [
    {
      suffix         = "-blue",
      ubuntu_version = "2004",
      size           = 5,
    },
    {
      suffix         = "-green",
      ubuntu_version = "2004",
      size           = 0,
    }
  ]

  es_ports = [
    { name = "es", port = "9200" },
    { name = "kibana", port = "5601" },
  ]
}

resource "google_compute_firewall" "es-ssh" {
  ## Disabled by default
  count   = 0
  name    = "es-ssh"
  network = google_compute_network.es.name
  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = [      # VPNs
    "35.194.81.56/32",   # North Virginia
    "35.189.40.124/32",  # Sydney
    "35.198.147.95/32",  # Frankfurt
    "18.210.210.130/32", # consultant
  ]
}

resource "google_compute_firewall" "es-http" {
  name        = "es-http"
  network     = google_compute_network.es.name
  target_tags = ["es"]

  source_ranges = [
    ## Google Load Balancer
    "130.211.0.0/22",
    "35.191.0.0/16",
  ]

  allow {
    protocol = "tcp"
    ports    = ["5601", "9200"]
  }
}

resource "google_compute_firewall" "es-internal" {
  name        = "es-internal"
  network     = google_compute_network.es.name
  target_tags = ["es"]

  source_ranges = [
    ## Internal
    "10.128.0.0/9",
  ]

  allow {
    protocol = "tcp"
    ports    = ["9300"]
  }
}


resource "google_service_account" "es" {
  account_id   = "elastic"
  display_name = "elastic"
}

resource "google_project_iam_custom_role" "es" {
  role_id     = "elastic"
  title       = "elastic"
  description = "elastic"
  permissions = [
    # Cloud logging
    "logging.logEntries.create",
    # ES discovery
    "compute.instances.get",
    "compute.instances.list",
  ]
}

resource "google_project_iam_member" "project" {
  project = local.project
  role    = google_project_iam_custom_role.es.id
  member  = "serviceAccount:${google_service_account.es.email}"
}

locals {
  es_startup_template = <<STARTUP
#! /bin/bash
set -euo pipefail
apt-get update
apt-get -y upgrade
### stackdriver
curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash

## Install Docker
apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io

## Set up ES

sysctl -w vm.max_map_count=262144

mkdir -p /root/es-docker
cd /root/es-docker

cat <<EOF > es.yml
cluster.name: es
node.name: $(hostname)
cluster.initial_master_nodes: %s
discovery.seed_providers: gce
discovery.gce.tags: es
cloud.gce.project_id: ${local.project}
cloud.gce.zone: ${local.zone}
network.host: 0.0.0.0
network.publish_host: _gce_
EOF

cat <<EOF > Dockerfile
FROM docker.elastic.co/elasticsearch/elasticsearch:7.13.2

RUN bin/elasticsearch-plugin install --batch discovery-gce
COPY es.yml /usr/share/elasticsearch/config/elasticsearch.yml
EOF

docker build -t es .
docker run -d \
           --name es \
           -p 9200:9200 \
           -p 9300:9300 \
           -e ES_JAVA_OPTS="-Xmx6g -Xms6g" \
           es

docker run -d \
           --name kibana \
           -p 5601:5601 \
           --link es:elasticsearch \
           -e TELEMETRY_ENABLED=false \
           docker.elastic.co/kibana/kibana:7.13.2

## Getting container output directly to the GCP console

( exec 1> >(while IFS= read -r line; do echo "elastic: $line"; done); docker logs -f es ) &
( exec 1> >(while IFS= read -r line; do echo "kibana: $line"; done); docker logs -f kibana ) &

for job in $(jobs -p); do
    wait $job
done

STARTUP

}

# ES v7 screwed up cluster initialization, so when starting from scratch we
# need a "special" node to kickstart the master election process. It can be
# killed as soon as the cluster is up and running.
resource "google_compute_instance" "es-init" {
  count        = 0
  name         = "es-init"
  machine_type = "e2-standard-2"
  tags         = ["es"]
  labels       = local.machine-labels
  zone         = local.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-${local.es_clusters[0].ubuntu_version}-lts"
    }
  }

  # This is the magic line that kickstarts the cluster formation process by
  # self-electing as master.
  metadata_startup_script = format(local.es_startup_template, "[\"$(hostname)\"]")

  network_interface {
    network = google_compute_network.es.name
    access_config {}
  }

  service_account {
    email = google_service_account.es.email
    scopes = [
      # Required for cloud logging
      "cloud-platform",
      # Required per ES documentation
      "compute-rw",
    ]
  }

}

resource "google_compute_instance_template" "es" {
  count        = length(local.es_clusters)
  name_prefix  = "es${local.es_clusters[count.index].suffix}-"
  machine_type = "e2-standard-2"
  tags         = ["es"]
  labels       = local.machine-labels

  disk {
    boot         = true
    disk_size_gb = 30
    source_image = "ubuntu-os-cloud/ubuntu-${local.es_clusters[count.index].ubuntu_version}-lts"
  }

  metadata_startup_script = format(local.es_startup_template, "[]")

  network_interface {
    network = google_compute_network.es.name
    access_config {}
  }

  service_account {
    email = google_service_account.es.email
    scopes = [
      # Required for cloud logging
      "cloud-platform",
      # Required per ES documentation
      "compute-rw",
    ]
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = true
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_instance_group_manager" "es" {
  provider           = google-beta
  count              = length(local.es_clusters)
  name               = "es${local.es_clusters[count.index].suffix}"
  base_instance_name = "es${local.es_clusters[count.index].suffix}"
  zone               = local.zone
  target_size        = local.es_clusters[count.index].size

  version {
    name              = "es${local.es_clusters[count.index].suffix}"
    instance_template = google_compute_instance_template.es[count.index].self_link
  }

  dynamic named_port {
    for_each = local.es_ports
    content {
      name = named_port.value["name"]
      port = named_port.value["port"]
    }
  }

  update_policy {
    type                    = "PROACTIVE"
    minimal_action          = "REPLACE"
    max_unavailable_percent = 100
  }
}

resource "google_compute_global_address" "es" {
  count      = length(local.es_ports)
  name       = "es-${local.es_ports[count.index].name}"
  ip_version = "IPV4"
}

resource "google_compute_health_check" "es-http" {
  count              = length(local.es_ports)
  name               = "es-http-${local.es_ports[count.index].name}"
  check_interval_sec = 10
  timeout_sec        = 1

  tcp_health_check {
    port = local.es_ports[count.index].port
  }
}

resource "google_compute_backend_service" "es-http" {
  count           = length(local.es_ports)
  name            = "es-http-${local.es_ports[count.index].name}"
  health_checks   = [google_compute_health_check.es-http[count.index].self_link]
  port_name       = local.es_ports[count.index].name
  security_policy = google_compute_security_policy.es.self_link

  dynamic backend {
    for_each = local.es_clusters
    content {
      group = google_compute_instance_group_manager.es[backend.key].instance_group
    }
  }
}

resource "google_compute_url_map" "es-http" {
  count           = length(local.es_ports)
  name            = "es-http-${local.es_ports[count.index].name}"
  default_service = google_compute_backend_service.es-http[count.index].self_link
}

resource "google_compute_target_http_proxy" "es-http" {
  count   = length(local.es_ports)
  name    = "es-http-${local.es_ports[count.index].name}"
  url_map = google_compute_url_map.es-http[count.index].self_link
}

resource "google_compute_global_forwarding_rule" "es_http" {
  count      = length(local.es_ports)
  name       = "es-http-${local.es_ports[count.index].name}"
  target     = google_compute_target_http_proxy.es-http[count.index].self_link
  ip_address = google_compute_global_address.es[count.index].address
  port_range = "80"
}

## The proxy implied by the forwarding rule sits outside our network, but we
## still want to limit to VPNs.
resource "google_compute_security_policy" "es" {
  name = "es"

  rule {
    action   = "deny(403)"
    priority = "2147483647"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["*"]
      }
    }
    description = "Default: deny all"
  }

  rule {
    action   = "allow"
    priority = "1000"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = [      # VPNs
          "35.194.81.56/32",   # North Virginia
          "35.189.40.124/32",  # Sydney
          "35.198.147.95/32",  # Frankfurt
          "18.210.210.130/32", # consultant
        ]
      }
    }
    description = "Allow VPNs"
  }
}

output "es_address" {
  value = google_compute_global_address.es[0].address
}

output "kibana_address" {
  value = google_compute_global_address.es[1].address
}

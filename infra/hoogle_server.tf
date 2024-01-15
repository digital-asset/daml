# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

resource "google_compute_network" "hoogle" {
  name = "hoogle-network"
}

resource "google_compute_firewall" "hoogle" {
  name        = "hoogle-firewall"
  network     = google_compute_network.hoogle.name
  target_tags = ["hoogle"]

  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]

  allow {
    protocol = "tcp"
    ports    = ["8080", "8081"]
  }
}

resource "google_compute_firewall" "hoogle-ssh" {
  count   = 0
  name    = "hoogle-ssh"
  network = google_compute_network.hoogle.name
  log_config {
    metadata = "INCLUDE_ALL_METADATA"
  }
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = [
    "35.194.81.56/32",  # North Virginia
    "35.189.40.124/32", # Sydney
    "35.198.147.95/32", # Frankfurt
  ]
}

locals {
  h_clusters = [
    {
      suffix         = "-blue",
      ubuntu_version = "2004",
      size           = 3,
    },
    {
      suffix         = "-green",
      ubuntu_version = "2004",
      size           = 0,
    }
  ]
}

resource "google_compute_instance_template" "hoogle" {
  count        = length(local.h_clusters)
  name_prefix  = "hoogle${local.h_clusters[count.index].suffix}-"
  machine_type = "n1-standard-1"
  tags         = ["hoogle"]
  labels       = local.machine-labels

  disk {
    boot         = true
    disk_size_gb = 20
    source_image = "ubuntu-os-cloud/ubuntu-${local.h_clusters[count.index].ubuntu_version}-lts"
  }

  metadata_startup_script = <<STARTUP
#! /bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get -y upgrade
### stackdriver
# Taken from https://cloud.google.com/logging/docs/agent/logging/installation
curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
curl -sSL https://dl.google.com/cloudagents/add-logging-agent-repo.sh | bash -s -- --also-install
### nginx
apt-get -y install nginx
cat > /etc/nginx/nginx.conf <<NGINX
user www-data;
worker_processes auto;
pid /run/nginx.pid;
events {
  worker_connections 768;
}
http {
  sendfile on;
  tcp_nopush on;
  tcp_nodelay on;
  keepalive_timeout 65;
  types_hash_max_size 2048;
  include /etc/nginx/mime.types;
  default_type application/octet-stream;
  access_log /var/log/nginx/access.log;
  error_log /var/log/nginx/error.log;
  server {
    listen 8081 default_server;
    server_name _;
    return 307 https://hoogle.daml.com\$request_uri;
  }
}
NGINX
service nginx restart
### hoogle
apt-get -y install curl git
useradd hoogle
mkdir /home/hoogle
chown hoogle:hoogle /home/hoogle
cd /home/hoogle
mkdir /nix
chown hoogle:hoogle /nix
runuser -l hoogle <<'HOOGLE_SETUP'
curl -sSfL https://nixos.org/nix/install | sh
. /home/hoogle/.nix-profile/etc/profile.d/nix.sh
# Feel free to bump the commit, this was the latest
# # at the time of creation.
export NIX_PATH=nixpkgs=https://github.com/NixOS/nixpkgs/archive/04dc69b62ec24a462585c1e705b711e2e05f3080.tar.gz
HOOGLE_PATH=$(nix-build --no-out-link -E '(import <nixpkgs> {}).haskellPackages.hoogle')
mkdir -p /home/hoogle/.local/bin
ln -s $HOOGLE_PATH/bin/hoogle /home/hoogle/.local/bin/hoogle
cat > /home/hoogle/refresh-db.sh <<MAKE_DB
#!/usr/bin/env bash
set -euo pipefail
log() {
  echo "[\$(date -Is)] \$1" >> /home/hoogle/cron_log.txt
}
log "Checking for new Daml version..."
cd /home/hoogle
mkdir new-daml
curl -s https://docs.daml.com/hoogle_db.tar.gz --output db.tar.gz
tar xzf db.tar.gz -C new-daml --strip-components=1
if ! diff -rq daml new-daml; then
  log "New version detected. Creating database..."
  rm -rf daml
  mv new-daml daml
  rm -f daml.hoo
  /home/hoogle/.local/bin/hoogle generate --database=daml.hoo --local=daml
  log "Killing running instance..."
  killall hoogle || true
  log "Starting new server..."
  nohup /home/hoogle/.local/bin/hoogle server --database=daml.hoo --log=.log.txt --port=8080 >> out.txt &
  log "New server started."
else
  log "No change detected."
  rm -rf new-daml
fi
log "Done."
MAKE_DB
chmod +x /home/hoogle/refresh-db.sh
./refresh-db.sh
echo "*/5 * * * * /home/hoogle/refresh-db.sh" | crontab -
echo "Successfully ran startup script."
tail -f cron_log.txt
HOOGLE_SETUP
STARTUP

  network_interface {
    network = google_compute_network.hoogle.name
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

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_instance_group_manager" "hoogle" {
  provider           = google-beta
  count              = length(local.h_clusters)
  name               = "hoogle${local.h_clusters[count.index].suffix}"
  base_instance_name = "hoogle${local.h_clusters[count.index].suffix}"
  zone               = local.zone
  target_size        = local.h_clusters[count.index].size

  version {
    name              = "hoogle${local.h_clusters[count.index].suffix}"
    instance_template = google_compute_instance_template.hoogle[count.index].self_link
  }

  named_port {
    name = "https"
    port = "8080"
  }

  named_port {
    name = "http"
    port = "8081"
  }

  auto_healing_policies {
    health_check = google_compute_health_check.hoogle-https.self_link

    # Compiling hoogle takes some time
    initial_delay_sec = 600
  }

  update_policy {
    type                  = "PROACTIVE"
    minimal_action        = "REPLACE"
    max_unavailable_fixed = 1
  }
}

resource "google_compute_global_address" "hoogle" {
  name       = "hoogle"
  ip_version = "IPV4"
}

resource "google_compute_health_check" "hoogle-http" {
  name               = "hoogle-http"
  check_interval_sec = 1
  timeout_sec        = 1

  tcp_health_check {
    port = 8081
  }
}

resource "google_compute_backend_service" "hoogle-http" {
  name          = "hoogle-http"
  health_checks = [google_compute_health_check.hoogle-http.self_link]
  port_name     = "http"

  dynamic "backend" {
    for_each = local.h_clusters
    content {
      group = google_compute_instance_group_manager.hoogle[backend.key].instance_group
    }
  }
}

resource "google_compute_url_map" "hoogle-http" {
  name            = "hoogle-http"
  default_service = google_compute_backend_service.hoogle-http.self_link
}

resource "google_compute_target_http_proxy" "hoogle-http" {
  name    = "hoogle-http"
  url_map = google_compute_url_map.hoogle-http.self_link
}

resource "google_compute_global_forwarding_rule" "hoogle_http" {
  name       = "hoogle-http"
  target     = google_compute_target_http_proxy.hoogle-http.self_link
  ip_address = google_compute_global_address.hoogle.address
  port_range = "80"
}

resource "google_compute_health_check" "hoogle-https" {
  name               = "hoogle-https"
  check_interval_sec = 1
  timeout_sec        = 1

  tcp_health_check {
    port = 8080
  }
}

resource "google_compute_backend_service" "hoogle-https" {
  name          = "hoogle-https"
  health_checks = [google_compute_health_check.hoogle-https.self_link]
  port_name     = "https"

  dynamic "backend" {
    for_each = local.h_clusters
    content {
      group = google_compute_instance_group_manager.hoogle[backend.key].instance_group
    }
  }
}

resource "google_compute_url_map" "hoogle-https" {
  name            = "hoogle-https"
  default_service = google_compute_backend_service.hoogle-https.self_link
}

resource "google_compute_target_https_proxy" "hoogle-https" {
  name    = "hoogle-https"
  url_map = google_compute_url_map.hoogle-https.self_link

  ssl_certificates = [local.ssl_certificate_hoogle]
  ssl_policy       = google_compute_ssl_policy.ssl_policy.self_link
}

resource "google_compute_global_forwarding_rule" "hoogle_https" {
  name       = "hoogle-https"
  target     = google_compute_target_https_proxy.hoogle-https.self_link
  ip_address = google_compute_global_address.hoogle.address
  port_range = "443"
}

output "hoogle_address" {
  value = google_compute_global_address.hoogle.address
}
